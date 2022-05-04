"""async_queues_test.py.

Test cases for bufferq.async_util
"""
import unittest
import sys
import asyncio
import inspect
from functools import wraps
from bufferq import AsyncQueue, AsyncLIFOQueue, AsyncPriorityQueue


def async_test(timeout=5):
    """Decorator that flags a function/method as an asynchronous test."""

    def _outer(func):
        if not inspect.iscoroutinefunction(func):
            raise TypeError("'{}' is not a coroutine!".format(func))

        @wraps(func)
        def new_func(*args, **kwargs):
            # Allocate an IOLoop.
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def _runner():
                await asyncio.wait_for(func(*args, **kwargs), timeout)

            try:
                loop.run_until_complete(_runner())
            finally:
                loop.close()
        return new_func
    return _outer


class AsyncQueueTest(unittest.TestCase):

    @async_test()
    async def test_async_queue(self):
        q = AsyncQueue()
        for i in range(10):
            await q.push(i)

        self.assertEqual(10, q.qsize())

        # Now, pop the items from the queue.
        for i in range(10):
            item = await q.pop()
            self.assertEqual(i, item)

        self.assertEqual(0, q.qsize())

    @async_test()
    async def test_async_queue_generator(self):
        q = AsyncQueue()

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

    @async_test()
    async def test_async_lifo_queue(self):
        q = AsyncLIFOQueue()

        for i in range(100):
            await q.push(i)

        # Stop the queue. It should still yield items until drained.
        await q.stop()

        expected = 99
        async for item in q.consume_one_generator():
            self.assertEqual(expected, item)
            expected -= 1

    @async_test()
    async def test_async_priority_queue(self):
        q = AsyncPriorityQueue()
        # Iterate from 100 -> 0
        for i in range(100, -1, -1):
            await q.push(i)

        # Stop the queue. It should still yield items until drained.
        await q.stop()

        expected = 0
        async for item in q.consume_one_generator():
            self.assertEqual(expected, item)
            expected += 1


if __name__ == '__main__':
    unittest.main()
