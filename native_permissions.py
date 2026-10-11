"""Pinned native Codex filesystem/network restrictions, shared by both roles."""
import json
from pathlib import Path

def profile(role, repo, binary, tmp):
    if role not in ('coding','review'): raise ValueError('Codex role required')
    rules={':root':'deny',':minimal':'read','/usr':'read',str(binary):'read',
        str(repo):'write' if role=='coding' else 'read',str(tmp):'write'}
    # More-specific rules preserve task/contract/Git metadata against coding edits.
    for leaf in ('.git',):
        rules[str(Path(repo)/leaf)]='read'
    return {'filesystem':rules,'network':{'enabled':False}}

def overrides(role, repo, binary, home, tmp):
    policy=profile(role,repo,binary,tmp)
    items={'approval_policy':'never','model_provider':'openai','forced_login_method':'chatgpt',
        'model_reasoning_effort':'high','web_search':'disabled','project_doc_max_bytes':0,
        'shell_environment_policy.inherit':'none',
        'shell_environment_policy.set':{'PATH':'/usr/bin:/bin','HOME':str(home),'TMPDIR':str(tmp)},
        'default_permissions':'simple-'+role,
        'permissions.simple-'+role:policy,
        'features.apps':False,'features.multi_agent':False,'features.multi_agent_v2':False,
        'features.memories':False,'features.hooks':False,'features.remote_plugin':False,
        'features.plugins':False,'features.skip_host_skill_discovery':True,'features.skill_search':False,
        'features.skill_mcp_dependency_install':False,'features.computer_use':False,
        'features.browser_use':False,'features.browser_use_external':False,'features.in_app_browser':False,
        'features.in_app_local_automation':False,'features.image_generation':False,
        'features.daemon_auto_start':False,'features.shell_snapshot':False,'features.workspace_dependencies':False,
        'features.unbounded_connection_retries':False,
        'mcp_servers':{},'notify':[]}
    # JSON syntax is not TOML for objects. Encode inline tables recursively.
    def toml(value):
        if isinstance(value,dict):return '{'+','.join(json.dumps(k)+'='+toml(v) for k,v in value.items())+'}'
        if isinstance(value,list):return '['+','.join(toml(v) for v in value)+']'
        return json.dumps(value)
    return [key+'='+toml(value) for key,value in items.items()]

