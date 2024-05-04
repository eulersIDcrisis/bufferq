"""async_queues_test.py.

Test cases for bufferq.async_util
"""
import unittest
import sys
import asyncio
import inspect
from functools import wraps
import bufferq


class AsyncQueueTest(unittest.IsolatedAsyncioTestCase):
    async def test_async_queue(self):
        q = bufferq.AsyncQueue()
        # Maxsize should be a value indicating unlimited.
        self.assertTrue(q.maxsize <= 0)
        for i in range(10):
            await q.push(i)

        self.assertEqual(10, q.qsize())

        # Now, pop the items from the queue.
        for i in range(10):
            item = await q.pop()
            self.assertEqual(i, item)

        self.assertEqual(0, q.qsize())

    async def test_async_queue_generator(self):
        q = bufferq.AsyncQueue()
        # Maxsize should be a value indicating unlimited.
        self.assertTrue(q.maxsize <= 0)

        async def producer():
            for i in range(1000):
                await q.push(i)
            await q.stop()

        async def consumer():
            count = 0
            async for item in q.consume_one_generator():
                self.assertEqual(count, item)
                count += 1

        producer_fut = asyncio.create_task(producer())
        consumer_fut = asyncio.create_task(consumer())
        await asyncio.gather(producer_fut, consumer_fut)

    async def test_async_queue_maxsize(self):
        q = bufferq.AsyncQueue(maxsize=2)
        await q.push(1)
        await q.push(2)
        self.assertTrue(q.full())
        with self.assertRaises(bufferq.QueueFull):
            await q.push(666)

        items = await q.pop_all()
        self.assertEqual([1, 2], list(items))

    async def test_async_lifo_queue(self):
        q = bufferq.AsyncLIFOQueue()
        # Maxsize should be a value indicating unlimited.
        self.assertTrue(q.maxsize <= 0)

        for i in range(100):
            await q.push(i)

        # Stop the queue. It should still yield items until drained.
        await q.stop()

        expected = 99
        async for item in q.consume_one_generator():
            self.assertEqual(expected, item)
            expected -= 1

    async def test_async_lifo_maxsize(self):
        q = bufferq.AsyncLIFOQueue(maxsize=2)
        await q.push(1)
        await q.push(2)
        self.assertTrue(q.full())
        with self.assertRaises(bufferq.QueueFull):
            await q.push(666)

        items = await q.pop_all()
        self.assertEqual([2, 1], list(items))

    async def test_async_priority_queue(self):
        q = bufferq.AsyncPriorityQueue()
        # Iterate from 100 -> 0
        for i in range(100, -1, -1):
            await q.push(i)

        # Stop the queue. It should still yield items until drained.
        await q.stop()

        expected = 0
        async for item in q.consume_one_generator():
            self.assertEqual(expected, item)
            expected += 1

    async def test_async_priority_queue_maxsize(self):
        q = bufferq.AsyncPriorityQueue(maxsize=2)
        await q.push(2)
        await q.push(1)

        with self.assertRaises(bufferq.QueueFull):
            await q.push(666)

        items = await q.pop_all()
        # Popping all of the items from the priority queue _might_ not
        # guarantee the sort order to save time, so manually guarantee
        # the sort order here.
        items.sort()
        self.assertEqual([1, 2], list(items))


if __name__ == "__main__":
    unittest.main()
