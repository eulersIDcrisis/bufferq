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
# Typing Imports
from typing import (
    Optional, Any, AsyncGenerator, Union, Sequence
)
# Common typing.
Number = Union[int, float]

# Standard Library Imports
import abc
import heapq
import asyncio
from collections import deque
# Local Imports
from bufferq.util import diff_time
from bufferq.errors import QueueStopped, QueueFull, QueueEmpty


class AsyncQueueBase(object, metaclass=abc.ABCMeta):
    """An asynchronous Queue, similar to bufferq.Queue.

    This queue is designed to be used in an async context.
    """

    def __init__(self, maxsize : int=None):
        self._maxsize = maxsize
        self._stopped = False
        self._lock = asyncio.Lock()
        self._empty_cond = asyncio.Condition(self._lock)
        self._full_cond = asyncio.Condition(self._lock)

    @property
    def maxsize(self) -> int:
        """Return the configured maximum size for this queue.

        If 'None' is returned, the size is presumed to be unlimited.
        """
        return self._maxsize

    async def stop(self) -> None:
        async with self._lock:
            self._stopped = True
            self._empty_cond.notify_all()
            self._full_cond.notify_all()

    async def push(self, item: Any, timeout: Number=None):
        remaining = timeout
        start_ts = diff_time()
        async with self._full_cond:
            while not self._stopped:
                try:
                    self._push_item(item)
                    # Notify the empty condition that an item has been added.
                    self._empty_cond.notify()
                    return
                except QueueFull as qf:
                    if remaining is None:
                        await self._full_cond.wait()
                    elif remaining > 0:
                        wait_fut = self._full_cond.wait()
                        try:
                            await asyncio.wait_for(wait_fut, remaining)
                        except asyncio.TimeoutError:
                            # Don't raise the timeout error, but raise the
                            # original QueueFull error.
                            raise qf
                        # Update the remaining time since we've waited a bit.
                        remaining = diff_time() - remaining
                    else:
                        raise

            # Only get here if the queue is stopped.
            raise QueueStopped('Cannot add item to stopped queue.')

    async def pop(self, timeout: Number=None) -> Any:
        remaining = timeout
        start_ts = diff_time()
        async with self._empty_cond:
            while True:
                try:
                    item = self._pop_item()
                    # Notify the full condition that an item has been removed.
                    self._full_cond.notify()
                    return item
                except QueueEmpty as qf:
                    # Exit the loop if there is nothing else to pop.
                    if self._stopped:
                        break
                    # Handle the timeout.
                    if remaining is None:
                        await self._empty_cond.wait()
                    elif remaining > 0:
                        try:
                            await asyncio.wait_for(
                                self._empty_cond.wait(),
                                remaining)
                        except asyncio.TimeoutError:
                            # Raise QueueEmpty instead of the timeout.
                            raise qf
                    else:
                        raise
            # Only get here if the queue is stopped AND empty.
            raise QueueStopped()

    async def consume_one_generator(self) -> AsyncGenerator[Any, None]:
        try:
            while True:
                item = await self.pop()
                yield item
        except QueueStopped:
            return

    def empty(self) -> bool:
        """Return if the queue is empty."""
        return self.qsize() == 0

    def full(self) -> bool:
        """Return if the queue is full."""
        if self.maxsize <= 0:
            return False
        return self.qsize() >= self.maxsize

    #
    # Required Overrideable Methods by Subclasses
    #
    @abc.abstractmethod
    def qsize(self) -> int:
        """Return the number of items currently in the queue."""
        return 0

    @abc.abstractmethod
    def _push_item(self, item: Any):
        """Push the given item onto the queue without blocking.

        This should push a single item onto the queue, or raise QueueFull
        if the item could not be added.

        Parameters
        ----------
        item: Any
            The item to push onto the queue.

        Raises
        ------
        QueueFull:
            Raised if the queue is full.
        QueueStopped:
            Raised if the queue is stopped.
        """
        pass

    @abc.abstractmethod
    def _pop_item(self) -> Any:
        """Pop an item from the queue without blocking.

        If no item is available, this should raise `QueueEmpty`.

        Raises
        ------
        QueueEmpty:
            Raised if the queue is full.
        QueueStopped:
            Raised if the queue is stopped.
        """
        pass


class AsyncQueue(AsyncQueueBase):
    """Asynchronous queue with a similar interface to bufferq.Queue.

    The elements of this queue are popped in the same order they are added
    (i.e. FIFO).
    """

    def __init__(self, maxsize: int = None):
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


class AsyncLIFOQueue(AsyncQueueBase):
    """Asynchronous queue with a similar interface to bufferq.LIFOQueue.

    The elements of this queue are popped in the reverse order they are added.
    """

    def __init__(self, maxsize: int = None):
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


class AsyncPriorityQueue(AsyncQueueBase):
    """Asynchronous queue with a similar interface to bufferq.PriorityQueue.

    The minimum/smallest element is the next item to be removed.
    """

    def __init__(self, maxsize: int =0):
        super(AsyncPriorityQueue, self).__init__(maxsize=maxsize)
        self._items: List[Any] = []

    def qsize(self) -> int:
        """Return the number of elements in the queue.

        NOTE: The result is _not_ thread-safe!
        """
        return len(self._items)

    def _push_item(self, item: Any):
        if self.maxsize > 0 and self.maxsize > len(self._items):
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
