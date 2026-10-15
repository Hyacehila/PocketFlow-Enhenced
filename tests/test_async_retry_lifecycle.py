import asyncio
import contextvars
import copy
import gc
import sys
import unittest
import weakref
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from pocketflow import AsyncNode


def tracked_retry_index():
    # Count retained integer values without depending on the storage format.
    class RetryIndex(int):
        live = 0

        def __new__(cls, value):
            instance = super().__new__(cls, value)
            cls.live += 1
            return instance

        def __del__(self):
            type(self).live -= 1

    return RetryIndex


class TestAsyncRetryLifecycle(unittest.TestCase):
    def test_assignment_does_not_keep_nodes_alive(self):
        for with_cycle in (False, True):
            with self.subTest(with_cycle=with_cycle):
                context = contextvars.Context()

                def assign():
                    references = []
                    for _ in range(100):
                        node = AsyncNode()
                        if with_cycle:
                            node.successors['default'] = node
                        node.cur_retry = 7
                        references.append(weakref.ref(node))
                    return references

                references = context.run(assign)
                gc.collect()
                self.assertTrue(all(ref() is None for ref in references))
                # Keep the context alive through the collection assertion.
                self.assertEqual(context.run(lambda: AsyncNode().cur_retry), 0)

    def test_alternating_writes_keep_only_the_latest_values(self):
        def assign():
            retry_index = tracked_retry_index()
            nodes = [AsyncNode(), AsyncNode()]
            for index in range(2000):
                nodes[index % 2].cur_retry = retry_index(index)
            gc.collect()
            self.assertEqual([node.cur_retry for node in nodes], [1998, 1999])
            self.assertEqual(retry_index.live, 2)

        contextvars.Context().run(assign)

    def test_next_write_removes_values_for_collected_nodes(self):
        def assign():
            retry_index = tracked_retry_index()
            nodes = [AsyncNode() for _ in range(20)]
            for node in nodes:
                node.cur_retry = retry_index(7)
            del node, nodes
            gc.collect()
            survivor = AsyncNode()
            survivor.cur_retry = retry_index(9)
            gc.collect()
            self.assertEqual(survivor.cur_retry, 9)
            self.assertEqual(retry_index.live, 1)

        contextvars.Context().run(assign)

    def test_copied_contexts_inherit_values_and_isolate_writes(self):
        node = AsyncNode()
        parent = contextvars.Context()
        parent.run(setattr, node, 'cur_retry', 7)
        child = parent.copy()
        self.assertEqual(child.run(lambda: node.cur_retry), 7)
        child.run(setattr, node, 'cur_retry', 2)
        self.assertEqual(parent.run(lambda: node.cur_retry), 7)
        self.assertEqual(child.run(lambda: node.cur_retry), 2)
        parent.run(setattr, node, 'cur_retry', 9)
        self.assertEqual(child.run(lambda: node.cur_retry), 2)

    def test_shallow_copy_has_its_own_counter(self):
        def assign():
            original = AsyncNode()
            original.cur_retry = 7
            clone = copy.copy(original)
            self.assertEqual(clone.cur_retry, 0)
            clone.cur_retry = 2
            self.assertEqual(original.cur_retry, 7)
            self.assertEqual(clone.cur_retry, 2)

        contextvars.Context().run(assign)

    def test_identity_does_not_require_node_hash_or_equality(self):
        class UnhashableNode(AsyncNode):
            __hash__ = None

            def __eq__(self, other):
                raise AssertionError('Retry state must use node identity')

        def assign():
            first, second = UnhashableNode(), UnhashableNode()
            first.cur_retry, second.cur_retry = 3, 8
            self.assertEqual(first.cur_retry, 3)
            self.assertEqual(second.cur_retry, 8)

        contextvars.Context().run(assign)


class TestAsyncRetryLifecycleInTasks(unittest.IsolatedAsyncioTestCase):
    async def test_callback_assignments_allow_collection_in_a_running_task(self):
        for phase in ('none', 'constructor', 'prep', 'post'):
            with self.subTest(phase=phase):
                class Processor(AsyncNode):
                    def __init__(self):
                        super().__init__()
                        if phase == 'constructor':
                            self.cur_retry = 0

                    async def prep_async(self, shared):
                        if phase == 'prep':
                            self.cur_retry = 0
                        return shared

                    async def exec_async(self, item):
                        await asyncio.sleep(0)
                        return item

                    async def post_async(self, shared, prep, result):
                        if phase == 'post':
                            self.cur_retry = 0
                        return result

                references = []
                for index in range(8):
                    node = Processor()
                    self.assertEqual(await node.run_async(index), index)
                    references.append(weakref.ref(node))
                    del node
                gc.collect()
                self.assertTrue(all(ref() is None for ref in references))

    async def test_child_tasks_inherit_values_and_isolate_manual_writes(self):
        node = AsyncNode()
        node.cur_retry = 9
        ready = [asyncio.Event(), asyncio.Event()]

        async def worker(index):
            self.assertEqual(node.cur_retry, 9)
            node.cur_retry = index + 1
            ready[index].set()
            await ready[1 - index].wait()
            return node.cur_retry

        results = await asyncio.wait_for(asyncio.gather(worker(0), worker(1)), 2)
        self.assertEqual(results, [1, 2])
        self.assertEqual(node.cur_retry, 9)

    async def test_context_captured_during_execution_does_not_retain_node(self):
        class Processor(AsyncNode):
            async def exec_async(self, item):
                return contextvars.copy_context()

            async def post_async(self, shared, prep, result):
                return result

        node = Processor()
        reference = weakref.ref(node)
        captured = await node.run_async(None)
        del node
        gc.collect()
        self.assertIsNone(reference())
        self.assertEqual(captured.run(lambda: AsyncNode().cur_retry), 0)

    async def test_nested_call_restores_values_of_both_nodes(self):
        class Inner(AsyncNode):
            async def exec_async(self, item):
                outer.cur_retry = 99
                self.cur_retry = 55
                await asyncio.sleep(0)
                return outer.cur_retry, self.cur_retry

        class Outer(AsyncNode):
            async def exec_async(self, item):
                nested = await inner._exec(None)
                return nested, self.cur_retry, inner.cur_retry

        outer, inner = Outer(), Inner()
        outer.cur_retry, inner.cur_retry = 7, 8
        self.assertEqual(await outer._exec(None), ((99, 55), 0, 8))
        self.assertEqual((outer.cur_retry, inner.cur_retry), (7, 8))


if __name__ == '__main__':
    unittest.main()
