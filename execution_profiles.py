"""Versioned execution policy. Catalogs are data; adapters own CLI syntax.

Cursor exposes an account-specific exploded model list. A configured option
combination must map to a listed variant: bracket syntax alone is not evidence
that a particular model supports those parameters.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

CATALOG_FILE = Path(__file__).with_name('execution_models.json')
TOKEN = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:/-]*\Z')
AGENTS = ('cursor', 'codex')



class PreflightRejected(ValueError):
    """Confirmed execution validation failure before any launch receipt."""


class RuntimeUnavailable(RuntimeError):
    """Transient probe failure; does not establish invalid execution policy."""


class StaleReviewInput(RuntimeError):
    """Confirmed source mismatch before any worker preparation or launch."""


def rejection(message):
    from redaction import redact
    return {'status': 'REJECTED', 'launch_started': False, 'error': redact(str(message))[:512]}


def check_preflight(result):
    if not isinstance(result, dict) or result.get('status') not in (
            'READY', 'REPLAY', 'REJECTED', 'UNAVAILABLE', 'STALE_INPUT', 'FENCED'):
        raise RuntimeError('Invalid execution preflight response')
    if result['status'] in ('STALE_INPUT', 'FENCED'):
        if result.get('launch_started') is not False:
            raise RuntimeError('Invalid no-launch proof')
        if result['status'] == 'FENCED':
            from worker_runtime import RequestFenced
            raise RequestFenced('Request was durably fenced before launch')
        raise StaleReviewInput('Review source changed before launch')
    if result['status'] == 'UNAVAILABLE':
        if result.get('launch_started') is not False:
            raise RuntimeError('Invalid preflight unavailability proof')
        raise RuntimeUnavailable('Runtime discovery unavailable; check agent connectivity and retry the same request')
    if result['status'] == 'REJECTED':
        if result.get('launch_started') is not False or not isinstance(result.get('error'), str) or not result['error'].strip():
            raise RuntimeError('Invalid prelaunch rejection proof')
        raise PreflightRejected(result['error'])
    return result

def token(value, where):
    if not isinstance(value, str) or not TOKEN.fullmatch(value):
        raise ValueError(f'{where} must be a non-empty identifier (no CLI/bracket syntax)')
    return value


def object_fields(value, allowed, required, where):
    if not isinstance(value, dict) or set(value) - set(allowed) or set(required) - set(value):
        raise ValueError(f'Invalid {where}: expected fields {sorted(allowed)}, required {sorted(required)}')


def options(value, where):
    if not isinstance(value, dict):
        raise ValueError(f'{where} must be an object')
    for key, item in value.items():
        token(key, where + ' option')
        if type(item) is not bool:
            token(item, where + '.' + key)
    return value


def validate_options(model, values, where):
    options(values, where)
    for key, value in values.items():
        allowed = model['supported_options'].get(key, [])
        if not any(type(value) is type(v) and value == v for v in allowed):
            raise ValueError(f'{where}: {model["agent"]}/{model["model_id"]} does not support '
                             f'{key}={value!r}; supported values: {allowed}')


def model_entry(catalog, agent, model_id):
    found = next((m for m in catalog['models'] if m['agent'] == agent and m['model_id'] == model_id), None)
    if found is None:
        raise ValueError(f'Model {agent}/{model_id} is absent from the execution catalog; add it to configuration')
    return found


def runtime_model(model, values):
    validate_options(model, values, 'Execution options')
    combinations = model.get('constraints', {}).get('allowed_combinations')
    if combinations is not None and values not in combinations:
        raise ValueError(f'Unsupported option combination for {model["model_id"]}: {values}')
    if model['agent'] == 'cursor':
        variant = next((v for v in model.get('variants', []) if v['options'] == values), None)
        if variant:
            return variant['runtime_model_id']
        if values:
            raise ValueError(f'No verified Cursor variant for {model["model_id"]} options {values}; '
                             'add an exact options/runtime_model_id mapping listed by agent models')
    return model['model_id']


def validate_catalog(catalog):
    object_fields(catalog, {'schema_version', 'models', 'profiles', 'defaults'},
                  {'schema_version', 'models', 'profiles', 'defaults'}, 'execution catalog')
    if type(catalog['schema_version']) is not int or catalog['schema_version'] != 1:
        raise ValueError('Unsupported execution catalog schema_version; expected 1')
    if not isinstance(catalog['models'], list) or not catalog['models']:
        raise ValueError('Execution catalog models must be a non-empty array')
    seen = set()
    for model in catalog['models']:
        object_fields(model, {'agent', 'model_id', 'display_name', 'enabled', 'supported_options',
                             'defaults', 'variants', 'constraints', 'capabilities', 'metadata'},
                      {'agent', 'model_id', 'display_name', 'enabled', 'supported_options', 'defaults'}, 'model')
        if model['agent'] not in AGENTS or type(model['enabled']) is not bool:
            raise ValueError('Invalid model agent/enabled')
        token(model['model_id'], 'model_id')
        if model['model_id'] == 'auto':
            raise ValueError('model_id auto is reserved for automatic selection; use a distinct catalog id '
                             'and map it to runtime_model_id auto when needed')
        if not isinstance(model['display_name'], str) or not model['display_name'].strip():
            raise ValueError('Model display_name must be non-empty')
        identity = (model['agent'], model['model_id'])
        if identity in seen:
            raise ValueError(f'Duplicate model {identity}')
        seen.add(identity)
        supported = model['supported_options']
        if not isinstance(supported, dict):
            raise ValueError('supported_options must be an object of allowed value arrays')
        for key, values in supported.items():
            if not isinstance(values, list) or not values:
                raise ValueError(f'supported_options.{key} must be a non-empty array')
            for value in values:
                options({key: value}, 'supported_options')
                if value == 'auto':
                    raise ValueError('auto is reserved for configured default selection, not a supported option value')
                if model['agent'] == 'codex' and key == 'effort' and (not isinstance(value, str) or not re.fullmatch(r'[a-z]+', value)):
                    raise ValueError('Codex effort values must be reasoning-effort names')
            if len({json.dumps(v) for v in values}) != len(values):
                raise ValueError(f'Duplicate values in supported_options.{key}')
        if model['agent'] == 'codex' and set(supported) - {'effort'}:
            raise ValueError('Codex adapter currently supports only effort; no options may be silently dropped')
        validate_options(model, model['defaults'], 'Model defaults')
        constraints = model.get('constraints', {})
        object_fields(constraints, {'allowed_combinations'}, set(), 'model constraints')
        if 'allowed_combinations' in constraints:
            if not isinstance(constraints['allowed_combinations'], list):
                raise ValueError('allowed_combinations must be an array')
            for combo in constraints['allowed_combinations']:
                validate_options(model, combo, 'Constraint combination')
        for field in ('capabilities', 'metadata'):
            if field in model and not isinstance(model[field], dict):
                raise ValueError(f'Model {field} must be an object (descriptive only)')
        variants = model.get('variants', [])
        if not isinstance(variants, list) or (variants and model['agent'] != 'cursor'):
            raise ValueError('variants must be an array for Cursor models only')
        combinations = set()
        for variant in variants:
            object_fields(variant, {'options', 'runtime_model_id'}, {'options', 'runtime_model_id'}, 'Cursor variant')
            validate_options(model, variant['options'], 'Cursor variant')
            token(variant['runtime_model_id'], 'runtime_model_id')
            key = json.dumps(variant['options'], sort_keys=True)
            if key in combinations:
                raise ValueError('Duplicate Cursor variant option combination')
            combinations.add(key)
        runtime_model(model, model['defaults'])
    if not isinstance(catalog['profiles'], dict):
        raise ValueError('profiles must be an object')
    for name, profile in catalog['profiles'].items():
        token(name, 'Profile name')
        object_fields(profile, {'agent', 'model', 'options'}, {'agent', 'model', 'options'}, 'profile')
        if profile['agent'] not in AGENTS:
            raise ValueError('Profile agent must be cursor or codex')
        model = model_entry(catalog, profile['agent'], profile['model'])
        runtime_model(model, {**model['defaults'], **options(profile['options'], 'Profile options')})
    defaults = catalog['defaults']
    object_fields(defaults, {'agent', 'models', 'review_profiles'}, {'agent', 'models'}, 'catalog defaults')
    if defaults['agent'] not in AGENTS or not isinstance(defaults['models'], dict) or set(defaults['models']) != set(AGENTS):
        raise ValueError('Defaults require an agent and a model for cursor and codex')
    for agent, model_id in defaults['models'].items():
        model_entry(catalog, agent, model_id)
    reviews = defaults.get('review_profiles', {})
    if not isinstance(reviews, dict) or set(reviews) - set(AGENTS):
        raise ValueError('review_profiles must map agents to profile names')
    for agent, name in reviews.items():
        if name not in catalog['profiles'] or catalog['profiles'][name]['agent'] != agent:
            raise ValueError(f'Invalid review profile for {agent}: {name}')
    return catalog


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'Duplicate JSON field {key}')
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError(f'Invalid JSON constant {value}')


def load_catalog(project=None):
    configured = (project or {}).get('execution_catalog')
    if configured is None:
        configured = os.environ.get('DEVORCHESTRATOR_MODEL_CATALOG')
    if configured is not None and (not isinstance(configured, str) or not configured.strip()):
        raise ValueError('execution_catalog / DEVORCHESTRATOR_MODEL_CATALOG must be a non-empty file path')
    path = Path(configured).expanduser() if configured is not None else CATALOG_FILE
    if not path.is_absolute():
        path = CATALOG_FILE.parent / path
    try:
        raw = path.read_text()
        catalog = validate_catalog(json.loads(raw, object_pairs_hook=unique_object, parse_constant=reject_constant))
    except (OSError, ValueError, TypeError, KeyError) as exc:
        raise ValueError(f'Invalid execution catalog {path}: {exc}') from exc
    return catalog, dict(path=str(path), schema_version=1,
                         sha256=hashlib.sha256(raw.encode()).hexdigest())


def resolve(project=None, agent=None, model=None, effort=None, execution_profile=None,
            execution_options=None, role='coding'):
    project = project or {}
    if role not in ('coding', 'review'):
        raise ValueError('Execution role must be coding or review')
    catalog, identity = load_catalog(project)
    for key, value in (('agent', agent), ('model', model), ('effort', effort), ('execution_profile', execution_profile)):
        if value is not None:
            token(value, key)
    if agent not in (None, 'auto', *AGENTS):
        raise ValueError('Agent must be cursor, codex or auto')
    explicit = dict(options(execution_options if execution_options is not None else {}, 'execution_options'))
    if effort is not None:
        if 'effort' in explicit and explicit['effort'] != effort:
            raise ValueError('Conflicting effort and execution_options.effort')
        explicit['effort'] = effort
    automatic_options = {k for k, v in explicit.items() if v == 'auto'}
    explicit = {k: v for k, v in explicit.items() if k not in automatic_options}
    fallback_agent = project.get('default_agent', catalog['defaults']['agent'])
    if fallback_agent not in AGENTS:
        raise ValueError('Project default_agent must be cursor or codex')
    name = execution_profile
    if name is None:
        if role == 'review':
            reviews = project.get('review_execution_profiles', catalog['defaults'].get('review_profiles', {}))
            if not isinstance(reviews, dict) or set(reviews) - set(AGENTS):
                raise ValueError('Project review_execution_profiles must map agents to profile names')
            for review_agent, profile_name in reviews.items():
                token(profile_name, f'Review profile for {review_agent}')
                review_profile = catalog['profiles'].get(profile_name)
                if review_profile is None:
                    raise ValueError(f'Unknown review profile {profile_name} for agent {review_agent}; '
                                     'choose a profile from the execution catalog')
                if review_profile['agent'] != review_agent:
                    raise ValueError(f'Review profile {profile_name} is for {review_profile["agent"]}, '
                                     f'but review_execution_profiles maps it to {review_agent}; choose a matching profile')
            name = reviews.get(agent if agent in AGENTS else fallback_agent)
        else:
            name = project.get('execution_profile')
    if name is not None:
        token(name, 'execution_profile')
    profile = catalog['profiles'].get(name) if name is not None else None
    if name is not None and profile is None:
        raise ValueError(f'Unknown execution_profile {name}; available: {sorted(catalog["profiles"])}')
    selected_agent = agent if agent in AGENTS else profile['agent'] if profile else fallback_agent
    if profile and profile['agent'] != selected_agent and execution_profile is None and role == 'coding':
        profile, name = None, None
    if profile and profile['agent'] != selected_agent:
        raise ValueError(f'Profile {name} is for {profile["agent"]}, but explicit agent is {selected_agent}; choose a matching profile')
    selected_model = model if model not in (None, 'auto') else profile['model'] if profile else catalog['defaults']['models'][selected_agent]
    entry = model_entry(catalog, selected_agent, selected_model)
    if not entry['enabled']:
        raise ValueError(f'Model {selected_agent}/{selected_model} is disabled; enable it in the execution catalog')
    unknown_auto = automatic_options - set(entry['supported_options'])
    if unknown_auto:
        raise ValueError(f'Unsupported automatic options for {selected_agent}/{selected_model}: {sorted(unknown_auto)}')
    resolved = {**entry['defaults'], **(profile['options'] if profile else {}), **explicit}
    if automatic_options - set(resolved):
        raise ValueError(f'No configured default/profile value for automatic options: {sorted(automatic_options - set(resolved))}')
    runtime_id = runtime_model(entry, resolved)
    sources = dict(agent='explicit' if agent in AGENTS else 'profile' if profile else 'auto' if agent == 'auto' else 'default',
                   model='explicit' if model not in (None, 'auto') else 'profile' if profile else 'auto' if model == 'auto' else 'default',
                   options={k: 'explicit' if k in explicit else 'profile' if profile and k in profile['options'] else
                            'auto' if k in automatic_options else 'default' for k in resolved})
    return dict(schema_version=1, agent=selected_agent, model=selected_model, options=resolved,
                runtime_model=runtime_id, execution_profile=name, decision_sources=sources,
                catalog=identity, adapter_version=1)



def runtime_probe(machine, command, capture):
    try:
        return capture(machine, command)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeUnavailable(f'Cannot validate runtime on {machine}: {command} unavailable or timed out; '
                         'check the installed agent and connectivity') from exc

def discover(machine, agent, capture):
    """Read-only probes on the execution host, never an agent turn."""
    executable = 'agent' if agent == 'cursor' else 'codex'
    result = {}
    for key, command in [('version', f'{executable} --version'), ('help', f'{executable} --help')]:
        probe = runtime_probe(machine, command, capture)
        if probe.returncode != 0 or not probe.stdout.strip():
            raise RuntimeUnavailable(f'Cannot validate {agent} runtime on {machine}: {command} failed')
        result[key] = probe.stdout.strip()
    if agent == 'cursor':
        probe = runtime_probe(machine, 'agent models', capture)
        if probe.returncode != 0:
            raise RuntimeUnavailable(f'Cannot discover Cursor models on {machine}; run agent models and check authentication/connectivity')
        result['models'] = sorted(set(re.findall(r'^([A-Za-z0-9][A-Za-z0-9._:/-]*) - ', probe.stdout, re.M)))
        if not result['models']:
            raise RuntimeUnavailable('Cursor model discovery returned no model IDs; inspect agent models output')
    return result


def validate_runtime(execution, capabilities):
    agent = execution['agent']
    if agent == 'cursor':
        if '--model <model>' not in capabilities['help'] or execution['runtime_model'] not in capabilities['models']:
            raise ValueError(f'Cursor {capabilities["version"]} does not advertise model/variant '
                             f'{execution["runtime_model"]}; update the catalog or run agent models on the execution host')
    elif '--model' not in capabilities['help'] or (execution['options'] and '--config' not in capabilities['help']):
        raise ValueError(f'Codex {capabilities["version"]} lacks the required model/config CLI support')
    else:
        if set(execution['options']) - {'effort'}:
            raise ValueError('Unsupported Codex execution options')
    return dict(version=capabilities['version'])


def launch_identity(project, machine, capture, **kwargs):
    execution = resolve(project, **kwargs)
    execution['runtime'] = validate_runtime(execution, discover(machine, execution['agent'], capture))
    return execution


def cli_options(execution):
    if execution['agent'] == 'cursor':
        return ['--model', execution['runtime_model']]
    args = ['-m', execution['model']]
    if 'effort' in execution['options']:
        args += ['-c', 'model_reasoning_effort=' + json.dumps(execution['options']['effort'])]
    return args


def resume_identity(meta, capture):
    """Catalog edits never re-resolve an existing execution identity."""
    execution = copy.deepcopy(meta.get('execution'))
    if execution is None and meta.get('agent') == 'cursor' and (meta.get('model') or meta.get('reasoning_effort')):
        raise ValueError('Legacy Cursor task with options has no validated execution identity; start a new task')
    if execution is not None:
        object_fields(execution, {'schema_version', 'agent', 'model', 'options', 'runtime_model',
                                  'execution_profile', 'decision_sources', 'catalog', 'adapter_version', 'runtime'},
                      {'schema_version', 'agent', 'model', 'options', 'runtime_model', 'adapter_version'},
                      'saved execution identity')
        options(execution['options'], 'Saved options')
        if execution['adapter_version'] != 1:
            raise ValueError('Unsupported saved adapter_version; resume blocked')
        if (execution.get('schema_version') != 1 or execution.get('agent') != meta['agent'] or
                execution.get('model') != meta.get('model') or execution.get('options', {}).get('effort') != meta.get('reasoning_effort')):
            raise ValueError('Saved execution identity disagrees with task metadata; resume blocked')
        token(execution['model'], 'Saved model')
        token(execution['runtime_model'], 'Saved runtime model')
        validate_runtime(execution, discover(meta['machine'], meta['agent'], capture))
    return execution
