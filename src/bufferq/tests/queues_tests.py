# Copyright (C) 2022 Aaron Gibson (eulersidcrisis@yahoo.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""queues_tests.py.

Test cases for bufferq.queues
"""
import unittest
import threading
from contextlib import contextmanager
# BufferQ imports.
from bufferq import queues


@contextmanager
def generate_on_thread(func):
    thd = threading.Thread(target=func)
    try:
        thd.daemon = True
        thd.start()
        yield thd
    finally:
        thd.join(timeout=5)


class SimpleQueueTests(unittest.TestCase):

    def test_basic_operations_sequential(self):
        q = queues.SimpleQueue()
        with self.assertRaises(queues.QueueEmpty):
            # Should raise, since there is nothing in the queue.
            q.pop(timeout=0)

        for i in range(100):
            q.put(i)

        # 100 elements in the queue at this point.
        self.assertEqual(100, q.qsize())
        items = q.pop_all()
        self.assertEqual(100, len(items))
        self.assertEqual(0, q.qsize())
        # Compare using zip and various iterators to ignore type differences.
        for expected, actual in zip(range(100), items):
            self.assertEqual(expected, actual)

        # No elements in the queue at this point.
        self.assertEqual(0, q.qsize())

        for i in range(0, 100, 2):
            # The size of the queue should be equal to 'i' as a hacky way to
            # test this.
            self.assertEqual(i, q.qsize())
            q.put_multi([i, i + 1])

        # Remove items, 5 at a time. We should be able to do this 20 times.
        # NOTE: This test assumes that pop_items(count=5) will always return
        # the maximum number of items up to 'count'.
        for i in range(20):
            items = q.pop_items(5)
            self.assertEqual(5, len(items))
            expected = list(range(5 * i, 5 * (i + 1)))
            self.assertEqual(expected, items)

    def test_basic_producer_consumer(self):
        q = queues.SimpleQueue()

        def generator():
            for i in range(100):
                q.put(i)
            for i in range(100, 200, 2):
                q.put_multi([i, i + 1])

        with generate_on_thread(generator):
            for i in range(100):
                item = q.pop(timeout=5)
                self.assertEqual(i, item)
            for i in range(100, 200):
                item = q.pop(timeout=5)
                self.assertEqual(i, item)
