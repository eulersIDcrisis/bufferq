"""async_util_test.py.

Test cases for bufferq.async_util
"""
import unittest
import sys
from bufferq.async_util.queues import AsyncQueue

if sys.version_info < (3, 8, 0):
    AsyncioTestCase = unittest.TestCase
else:
    AsyncioTestCase = unittest.IsolatedAsyncioTestCase


@unittest.skipIf(sys.version_info < (3, 8, 0), "Cannot run on python < 3.8")
class AsyncQueueTest(AsyncioTestCase):

    async def test_async_queue(self):
        q = AsyncQueue()
        for i in range(10):
            await q.push(i)

        # Now, pop the items from the queue.
        for i in range(10):
            item = await q.pop()
            self.assertEqual(i, item)

    async def test_async_queue_generator(self):
        q = AsyncQueue()
        pass


if __name__ == '__main__':
    unittest.main()
