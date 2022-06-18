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
import random
import unittest
import threading
from contextlib import contextmanager
# BufferQ imports.
import bufferq


@contextmanager
def generate_on_thread(func):
    thd = threading.Thread(target=func)
    try:
        thd.daemon = True
        thd.start()
        yield thd
    finally:
        thd.join(timeout=5)


class QueueBaseTests(unittest.TestCase):

    def test_basic_operations_sequential(self):
        q = bufferq.Queue()
        with self.assertRaises(bufferq.QueueEmpty):
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

    def test_stop_and_reset(self):
        q = bufferq.Queue()

        q.put('a')
        q.stop()
        with self.assertRaises(bufferq.QueueStopped):
            q.put('a')

        item = q.get(timeout=1)
        self.assertEqual('a', item)

        # This should raise QueueStopped to flag that the queue is both
        # empty and stopped.
        with self.assertRaises(bufferq.QueueStopped):
            q.get(timeout=0)

        q.reset()
        # After reset, this should succeed again.
        q.put('b')

        item = q.get(timeout=0)
        self.assertEqual('b', item)

    def test_basic_producer_consumer(self):
        q = bufferq.Queue()

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

    def test_maxsize_queue(self):
        # Create a maximum queue with a size of 2.
        q = bufferq.Queue(maxsize=2)
        q.put(1)
        q.put(2)

        with self.assertRaises(bufferq.QueueFull):
            q.put(666)

        res = q.get_all()
        # Use assertSequenceEqual because the result might not be a list.
        self.assertSequenceEqual([1, 2], res)

        try:
            q.put_multi([1, 2, 3])
            self.fail('Expected call to raise QueueFull!')
        except bufferq.QueueFull as exc:
            self.assertSequenceEqual([3], exc.remaining_items)
        except Exception:
            # Test handler should catch the explicit exception here.
            raise

        res = q.get_all()
        self.assertSequenceEqual([1, 2], res)


class LIFOQueueTests(unittest.TestCase):

    def test_lifo_queue(self):
        q = bufferq.LIFOQueue()

        for i in range(5):
            q.put(i)

        # Elements should pop in the reverse order.
        for i in range(4, -1, -1):
            self.assertEqual(i, q.pop())

        # No more elements.
        with self.assertRaises(bufferq.QueueEmpty):
            q.pop(timeout=0)


class PriorityQueueTests(unittest.TestCase):

    def test_priority_queue(self):
        q = bufferq.PriorityQueue()

        items = list(range(20))
        random.shuffle(items)

        q.put_multi(items)

        # The items should be returned in order due to the relative priority.
        for i in range(20):
            self.assertFalse(q.empty())
            self.assertEqual(i, q.pop(timeout=0))

        self.assertTrue(q.empty())


def _consume_one_func(q, result_list):
    for item in q.consume_one_generator():
        result_list.append(item)


class ConcurrentTests(unittest.TestCase):

    def test_consume_one_generator(self):
        q = bufferq.Queue()

        thd1_list = []
        thd1 = threading.Thread(
            target=_consume_one_func, args=(q, thd1_list))
        thd1.daemon = True

        thd2_list = []
        thd2 = threading.Thread(
            target=_consume_one_func, args=(q, thd2_list))
        thd2.daemon = True

        # Start the consumers.
        thd1.start()
        thd2.start()

        for i in range(10000):
            q.push(i)

        # Done producing items, so stop the queue.
        q.stop()

        # Join the consumer threads as well. They should terminate cleanly.
        thd1.join()
        thd2.join()

        # Assert that the combined lists have the items added by the producer.
        # The items should appear to be monotonically increasing for the lists.
        self.assertTrue(all(x < y for x, y in zip(thd1_list, thd1_list[1:])))
        self.assertTrue(all(x < y for x, y in zip(thd2_list, thd2_list[1:])))

        combined_list = thd1_list + thd2_list
        combined_list.sort()
        self.assertSequenceEqual(list(range(10000)), combined_list)
