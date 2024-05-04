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
"""queues.py.

Base implementation module for the iterator-style Queues.
"""
# Typing Imports
from typing import Sequence, Any, Deque, List

# Standard Library Imports
import heapq
from collections import deque

# Local imports
import bufferq.errors as errors
from bufferq.base import AbstractQueue


#
# Implementations for Common Queue Types
#
class Queue(AbstractQueue):
    """Basic FIFO queue implementation."""

    def __init__(self, maxsize: int = 0):
        super(Queue, self).__init__(maxsize=maxsize)
        self._items: Deque[Any] = deque()

    def qsize(self) -> int:
        """Return the number of elements in the queue.

        NOTE: The result is _not_ thread-safe!
        """
        return len(self._items)

    def _push_item(self, item: Any):
        if self._maxsize <= 0:
            self._items.append(item)
        elif len(self._items) == self.maxsize:
            raise errors.QueueFull()
        else:
            # There is room. Add the item.
            self._items.append(item)

    def _pop_item(self) -> Any:
        if not self._items:
            raise errors.QueueEmpty()
        return self._items.popleft()

    def _pop_all(self) -> Sequence[Any]:
        # Just swap out a new deque for speed.
        result = self._items
        self._items = deque()
        return result


class LIFOQueue(Queue):
    """A Last-In, First-out queue (i.e. a Stack)."""

    # NOTE: Implementation is identical to a generic Queue, except that
    # elements are popped from the same side as they are added.
    def _pop_item(self) -> Any:
        if not self._items:
            raise errors.QueueEmpty()
        return self._items.pop()

    def _pop_all(self) -> Sequence[Any]:
        result = self._items
        self._items = deque()
        # Return the items in the reversed order.
        result.reverse()
        return result


class PriorityQueue(AbstractQueue):
    """Priority Queue implementation.

    Internally manages items via a simple heap.
    """

    def __init__(self, maxsize: int = 0):
        super(PriorityQueue, self).__init__(maxsize=maxsize)
        self._items: List[Any] = []

    def qsize(self) -> int:
        """Return the number of elements in the queue.

        NOTE: The result is _not_ thread-safe!
        """
        return len(self._items)

    def _push_item(self, item):
        if self.maxsize > 0 and len(self._items) >= self.maxsize:
            raise errors.QueueFull()
        heapq.heappush(self._items, item)

    def _pop_item(self) -> Any:
        if not self._items:
            raise errors.QueueEmpty()
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
