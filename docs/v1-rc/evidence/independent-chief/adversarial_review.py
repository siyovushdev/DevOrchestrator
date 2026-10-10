import os, sys, unittest
from contextlib import contextmanager
from unittest.mock import patch, Mock
import task_bridge as b
import bridge_effects as e
import chief_review
import test_bridge_integration as integration
from tests_helpers import coding_result, review_result

class IndependentBoundaryReview(unittest.IsolatedAsyncioTestCase):
    setUp = integration.BridgeIntegrationTests.setUp
    async def bundle(self):
        await b.tick(self.h.state)
        item=self.h.state['issues']['1']
        self.h.finish(item['task_id'],coding_result())
        await b.tick(self.h.state)
        self.h.finish(item['review_task_id'],review_result())
        with patch.object(b,'flush_effects'):
            await b.tick(self.h.state)
        keys=item['chief_review']['effects']
        self.h.state['outbox']={key:self.h.state['outbox'][key] for key in keys}
        b.save_state(self.h.state)
        return item,keys
    async def test_release_blocking_error_is_not_no_send_and_receipts_reconcile(self):
        item,keys=await self.bundle()
        @contextmanager
        def release_failure(*args,**kwargs):
            self.assertFalse(kwargs['blocking'])
            yield
            raise BlockingIOError('release error after POST')
        with patch.object(b,'exclusive',release_failure), patch.object(self.h,'post_comment',wraps=self.h.post_comment) as post:
            b.flush_effects(self.h.state,control_fresh={'1'})
            self.assertEqual(post.call_count,len(keys))
        self.assertTrue(all(self.h.state['outbox'][key]['status']=='UNCERTAIN' for key in keys))
        self.h.restart()
        with patch.object(self.h,'post_comment',side_effect=AssertionError('duplicate POST')):
            b.flush_effects(self.h.state,control_fresh={'1'})
        self.assertTrue(all(self.h.state['outbox'][key]['status']=='DONE' for key in keys))
        self.assertFalse(chief_review.approval_complete(self.h.state['issues']['1']))
        self.assertIsNone(self.h.state['issues']['1'].get('owner_state'))
    async def test_blocking_error_in_locked_tree_check_stays_uncertain(self):
        item,keys=await self.bundle()
        original=b.chief_tree_current_async
        count=0
        async def tree(*args,**kwargs):
            nonlocal count
            count+=1
            if count%2==0:
                raise BlockingIOError('tree failure before POST')
            return await original(*args,**kwargs)
        with patch.object(b,'chief_tree_current_async',side_effect=tree),patch.object(self.h,'post_comment',wraps=self.h.post_comment) as post:
            b.flush_effects(self.h.state,control_fresh={'1'})
            post.assert_not_called()
        self.assertTrue(all(self.h.state['outbox'][key]['status']=='UNCERTAIN' for key in keys))
        self.h.restart()
        with patch.object(self.h,'post_comment',side_effect=AssertionError('unsafe retry')):
            b.flush_effects(self.h.state,control_fresh={'1'})
        self.assertTrue(all(self.h.state['outbox'][key]['status']=='UNCERTAIN' for key in keys))
    @unittest.skipUnless(hasattr(e,'observation_held'),'Safety Latch additive module only')
    async def test_chief_quarantine_cannot_send_or_reconcile_after_latch_removed(self):
        item,keys=await self.bundle()
        import external_effects
        with patch.dict(os.environ,{external_effects.ENV:'1'}):
            held=e.enqueue(self.h.state,1,'chief_comment','independent held evidence',event='independent-held')
        entry=self.h.state['outbox'][held]
        self.assertEqual(entry['status'],'QUARANTINED')
        self.assertTrue(e.observation_held(self.h.state,1))
        b.save_state(self.h.state)
        self.h.restart()
        self.h.state['outbox']={held:self.h.state['outbox'][held]}
        self.h.state['outbox'][held]['status']='PENDING'
        callbacks=Mock(side_effect=AssertionError('quarantined callback'))
        e.flush(self.h.state,lambda:b.save_state(self.h.state),callbacks,callbacks,callbacks,callbacks,now=100,eligible=callbacks)
        callbacks.assert_not_called()
        self.assertEqual(self.h.state['outbox'][held]['effect_origin'],'safety_latch')
        self.assertFalse(chief_review.approval_complete(self.h.state['issues']['1']))
        self.assertIsNone(self.h.state['issues']['1'].get('owner_state'))

if __name__=='__main__': unittest.main(verbosity=2)
