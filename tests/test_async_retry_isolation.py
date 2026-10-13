import asyncio
import copy
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent))
from pocketflow import AsyncFlow, AsyncNode, AsyncParallelBatchNode


class TestAsyncRetryIsolation(unittest.IsolatedAsyncioTestCase):
    async def test_parallel_items_keep_independent_retry_budgets(self):
        for use_flow in (False, True):
            for wait in (0, 0.001):
                with self.subTest(use_flow=use_flow, wait=wait):
                    calls = {'a': 0, 'b': 0}
                    observed = {'a': [], 'b': []}
                    identities = set()
                    b_finished = asyncio.Event()

                    class Processor(AsyncParallelBatchNode):
                        async def prep_async(self, shared):
                            return ['a', 'b']

                        async def exec_async(self, item):
                            identities.add(id(self))
                            calls[item] += 1
                            attempt = calls[item]
                            before = self.cur_retry
                            if item == 'a' and attempt == 1:
                                await b_finished.wait()
                            observed[item].append((before, self.cur_retry))
                            if item == 'a' and attempt == 1 or item == 'b' and attempt < 3:
                                raise RuntimeError('temporary failure')
                            if item == 'b':
                                b_finished.set()
                            return item + '-ok'

                        async def exec_fallback_async(self, item, exc):
                            return item + '-fallback'

                        async def post_async(self, shared, prep, result):
                            shared['results'] = result

                    shared = {}
                    node = Processor(max_retries=3, wait=wait)
                    runner = AsyncFlow(start=node) if use_flow else node
                    await asyncio.wait_for(runner.run_async(shared), 2)
                    self.assertEqual(shared['results'], ['a-ok', 'b-ok'])
                    self.assertEqual(calls, {'a': 2, 'b': 3})
                    self.assertEqual(observed, {'a': [(0, 0), (1, 1)],
                                                'b': [(0, 0), (1, 1), (2, 2)]})
                    self.assertEqual(len(identities), 1)

    async def test_final_failure_always_reaches_fallback(self):
        for default_fallback in (False, True):
            with self.subTest(default_fallback=default_fallback):
                calls = {'a': 0, 'b': 0}
                fallbacks = []
                a_final = asyncio.Event()
                b_started = asyncio.Event()
                a_failed = asyncio.Event()

                class Processor(AsyncParallelBatchNode):
                    async def prep_async(self, shared):
                        return ['a', 'b']

                    async def exec_async(self, item):
                        calls[item] += 1
                        if item == 'a':
                            if calls[item] == 3:
                                a_final.set()
                                await b_started.wait()
                                a_failed.set()
                            raise RuntimeError('a final failure')
                        await a_final.wait()
                        b_started.set()
                        await a_failed.wait()
                        return 'b-ok'

                    async def exec_fallback_async(self, item, exc):
                        fallbacks.append((item, self.cur_retry))
                        if default_fallback:
                            return await super().exec_fallback_async(item, exc)
                        return 'a-fallback'

                    async def post_async(self, shared, prep, result):
                        return result

                node = Processor(max_retries=3)
                if default_fallback:
                    with self.assertRaisesRegex(RuntimeError, 'a final failure'):
                        await asyncio.wait_for(node.run_async({}), 2)
                else:
                    result = await asyncio.wait_for(node.run_async({}), 2)
                    self.assertEqual(result, ['a-fallback', 'b-ok'])
                self.assertEqual(calls, {'a': 3, 'b': 1})
                self.assertEqual(fallbacks, [('a', 2)])

    async def test_fallback_keeps_its_retry_index_across_await(self):
        a_in_fallback = asyncio.Event()
        b_started = asyncio.Event()
        observed = []

        class Processor(AsyncParallelBatchNode):
            async def prep_async(self, shared):
                return ['a', 'b']

            async def exec_async(self, item):
                if item == 'a':
                    raise RuntimeError('a failure')
                await a_in_fallback.wait()
                b_started.set()
                return 'b-ok'

            async def exec_fallback_async(self, item, exc):
                before = self.cur_retry
                a_in_fallback.set()
                await b_started.wait()
                observed.append((before, self.cur_retry))
                return 'a-fallback'

            async def post_async(self, shared, prep, result):
                return result

        result = await asyncio.wait_for(Processor(max_retries=3).run_async({}), 2)
        self.assertEqual(result, ['a-fallback', 'b-ok'])
        self.assertEqual(observed, [(2, 2)])

    async def test_concurrent_runs_on_one_async_node_are_isolated(self):
        calls = {'a': 0, 'b': 0}
        b_finished = asyncio.Event()

        class Processor(AsyncNode):
            async def prep_async(self, shared):
                return shared['item']

            async def exec_async(self, item):
                calls[item] += 1
                attempt = calls[item]
                if item == 'a' and attempt == 1:
                    await b_finished.wait()
                if item == 'a' and attempt == 1 or item == 'b' and attempt < 3:
                    raise RuntimeError('temporary failure')
                if item == 'b':
                    b_finished.set()
                return item + '-ok'

            async def exec_fallback_async(self, item, exc):
                return item + '-fallback'

            async def post_async(self, shared, prep, result):
                return result

        node = Processor(max_retries=3)
        results = await asyncio.wait_for(asyncio.gather(
            node.run_async({'item': 'a'}), node.run_async({'item': 'b'})
        ), 2)
        self.assertEqual(results, ['a-ok', 'b-ok'])
        self.assertEqual(calls, {'a': 2, 'b': 3})

    async def test_nested_run_restores_outer_retry_state(self):
        for use_copy in (False, True):
            with self.subTest(use_copy=use_copy):
                calls = 0
                observed = []

                class Processor(AsyncNode):
                    async def prep_async(self, shared):
                        return shared['item']

                    async def exec_async(self, item):
                        nonlocal calls
                        if item == 'inner':
                            observed.append(('inner', self.cur_retry, outer.cur_retry))
                            return 'inner-ok'
                        calls += 1
                        if calls == 2:
                            before = self.cur_retry
                            inner = copy.copy(self) if use_copy else self
                            result = await inner.run_async({'item': 'inner'})
                            observed.append(('outer', before, self.cur_retry, result))
                        raise RuntimeError('outer failure')

                    async def exec_fallback_async(self, item, exc):
                        observed.append(('fallback', self.cur_retry))
                        return 'outer-fallback'

                    async def post_async(self, shared, prep, result):
                        return result

                outer = Processor(max_retries=2)
                result = await asyncio.wait_for(outer.run_async({'item': 'outer'}), 2)
                self.assertEqual(result, 'outer-fallback')
                self.assertEqual(calls, 2)
                self.assertEqual(observed, [('inner', 0, 1 if use_copy else 0),
                                           ('outer', 1, 1, 'inner-ok'), ('fallback', 1)])

    async def test_success_and_exception_restore_callers_counter(self):
        class Processor(AsyncNode):
            async def exec_async(self, item):
                if item != 'success':
                    raise RuntimeError('final failure')
                return self.cur_retry

            async def exec_fallback_async(self, item, exc):
                if item == 'fallback-ok':
                    return 'recovered'
                if item == 'fallback-error':
                    raise ValueError('fallback failure')
                return await super().exec_fallback_async(item, exc)

        node = Processor(max_retries=2)
        node.cur_retry = 9
        self.assertEqual(await node._exec('success'), 0)
        self.assertEqual(node.cur_retry, 9)
        with self.assertRaisesRegex(RuntimeError, 'final failure'):
            await node._exec('fail')
        self.assertEqual(node.cur_retry, 9)
        self.assertEqual(await node._exec('fallback-ok'), 'recovered')
        self.assertEqual(node.cur_retry, 9)
        with self.assertRaisesRegex(ValueError, 'fallback failure'):
            await node._exec('fallback-error')
        self.assertEqual(node.cur_retry, 9)
        self.assertEqual(Processor().cur_retry, 0)

    async def test_cancellation_propagates_and_restores_callers_counter(self):
        for phase in ('exec', 'wait', 'fallback'):
            with self.subTest(phase=phase):
                started = asyncio.Event()
                observed = []
                calls = {'exec': 0, 'fallback': 0}

                async def block_until_cancelled():
                    observed.append(node.cur_retry)
                    started.set()
                    await asyncio.Future()

                class Processor(AsyncNode):
                    async def exec_async(self, item):
                        calls['exec'] += 1
                        if phase == 'exec':
                            await block_until_cancelled()
                        raise RuntimeError('failure')

                    async def exec_fallback_async(self, item, exc):
                        calls['fallback'] += 1
                        await block_until_cancelled()

                node = Processor(max_retries=1 if phase == 'fallback' else 3,
                                 wait=1 if phase == 'wait' else 0)
                node.cur_retry = 9

                async def retry_sleep(delay):
                    self.assertEqual(delay, 1)
                    await block_until_cancelled()

                async def run_and_observe_cancellation():
                    try:
                        await node._exec(None)
                    except asyncio.CancelledError:
                        observed.append(node.cur_retry)
                        raise

                with patch('pocketflow.asyncio.sleep', retry_sleep):
                    task = asyncio.create_task(run_and_observe_cancellation())
                    try:
                        await asyncio.wait_for(started.wait(), 2)
                    finally:
                        task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await task
                self.assertEqual(observed, [0, 9])
                self.assertEqual(node.cur_retry, 9)
                self.assertEqual(calls, {'exec': 1, 'fallback': int(phase == 'fallback')})

    async def test_post_receives_restored_counter(self):
        for callers_counter in (0, 9):
            with self.subTest(callers_counter=callers_counter):
                calls = 0

                class Processor(AsyncNode):
                    async def exec_async(self, item):
                        nonlocal calls
                        calls += 1
                        if calls == 1:
                            raise RuntimeError('temporary failure')
                        return self.cur_retry

                    async def post_async(self, shared, prep, result):
                        return result, self.cur_retry

                node = Processor(max_retries=3)
                if callers_counter:
                    node.cur_retry = callers_counter
                self.assertEqual(await node.run_async({}), (1, callers_counter))
                self.assertEqual(node.cur_retry, callers_counter)

    async def test_nested_failure_restores_outer_retry_state(self):
        for use_copy in (False, True):
            with self.subTest(use_copy=use_copy):
                calls = {'outer': 0, 'inner': 0}
                observed = []

                class Processor(AsyncNode):
                    async def exec_async(self, item):
                        calls[item] += 1
                        if item == 'inner':
                            observed.append(('inner', self.cur_retry, outer.cur_retry))
                            raise ValueError('inner failure')
                        if calls[item] == 1:
                            inner = copy.copy(self) if use_copy else self
                            try:
                                await inner._exec('inner')
                            except ValueError:
                                observed.append(('restored', self.cur_retry))
                        raise RuntimeError('outer failure')

                    async def exec_fallback_async(self, item, exc):
                        if item == 'inner':
                            raise exc
                        return self.cur_retry

                outer = Processor(max_retries=2)
                self.assertEqual(await outer._exec('outer'), 1)
                self.assertEqual(calls, {'outer': 2, 'inner': 2})
                self.assertEqual(observed, [('inner', 0, 0),
                                           ('inner', 1, 0 if use_copy else 1), ('restored', 0)])

    async def test_public_counter_cannot_change_the_retry_budget(self):
        calls = 0

        class Processor(AsyncNode):
            async def exec_async(self, item):
                nonlocal calls
                calls += 1
                self.cur_retry = 99
                raise RuntimeError('failure')

            async def exec_fallback_async(self, item, exc):
                return 'fallback'

        result = await Processor(max_retries=3)._exec(None)
        self.assertEqual(result, 'fallback')
        self.assertEqual(calls, 3)


if __name__ == '__main__':
    unittest.main()
