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
"""async_queues.py.

Module implementing some common (asynchronous) buffering utilities.
"""
from collections.abc import Sequence
from typing import Any

# Standard Library Imports
import heapq
from collections import deque

# Local Imports
from bufferq.errors import QueueEmpty, QueueFull
from bufferq.base import AsyncAbstractQueue


class AsyncQueue(AsyncAbstractQueue):
    """Asynchronous queue with a similar interface to bufferq.Queue.

    The elements of this queue are popped in the same order they are added
    (i.e. FIFO).
    """

    def __init__(self, maxsize: int = 0):
        super(AsyncQueue, self).__init__(maxsize=maxsize)
        self._items = deque()

    def qsize(self) -> int:
        """Return the number of elements in the queue.

        NOTE: The result is _not_ thread-safe!
        """
        return len(self._items)

    def _push_item(self, item):
        if self.maxsize and len(self._items) >= self.maxsize:
            raise QueueFull()
        self._items.append(item)

    def _pop_item(self):
        if self._items:
            return self._items.popleft()
        raise QueueEmpty()


class AsyncLIFOQueue(AsyncQueue):
    """Asynchronous queue with a similar interface to bufferq.LIFOQueue.

    The elements of this queue are popped in the reverse order they are added.
    """

    def __init__(self, maxsize: int = 0):
        super(AsyncLIFOQueue, self).__init__(maxsize=maxsize)
        self._items = deque()

    def qsize(self) -> int:
        """Return the number of elements in the queue.

        NOTE: The result is _not_ thread-safe!
        """
        return len(self._items)

    def _push_item(self, item):
        if self.maxsize and len(self._items) >= self.maxsize:
            raise QueueFull()
        self._items.append(item)

    def _pop_item(self):
        if self._items:
            return self._items.pop()
        raise QueueEmpty()


class AsyncPriorityQueue(AsyncAbstractQueue):
    """Asynchronous queue with a similar interface to bufferq.PriorityQueue.

    The minimum/smallest element is the next item to be removed.
    """

    def __init__(self, maxsize: int = 0):
        super(AsyncPriorityQueue, self).__init__(maxsize=maxsize)
        self._items = []

    def qsize(self) -> int:
        """Return the number of elements in the queue.

        NOTE: The result is _not_ thread-safe!
        """
        return len(self._items)

    def _push_item(self, item: Any):
        if self.maxsize > 0 and self.maxsize <= len(self._items):
            raise QueueFull()
        heapq.heappush(self._items, item)

    def _pop_item(self) -> Any:
        if not self._items:
            raise QueueEmpty()
        return heapq.heappop(self._items)

    def _pop_all(self) -> Sequence[Any]:
        # Just swap out the list.
        # TODO: Should the items actually be sorted? Most cases that pop
        # all of the elements might not strictly care about the exact order
        # of the full set of items once popped; they can sort them outside
        # of any locking.
        result = self._items
        self._items = []
        return result
