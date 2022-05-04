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

    #
    # Required Overrideable Methods by Subclasses
    #
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


class BufferedStream(object):
    """Queue-like stream of bytes for asynchronous contexts.

    This class is similar to a Queue but instead of popping individual
    bytes, this pops items from the buffer of the requested size in an
    optimized manner. Like with the 'bufferq.Queue', this supports the
    ability to drain the stream before closing.
    """

    def __init__(self, buffsize):
        self._buffsize = buffsize
        self._buffer = bytearray(buffsize)
        self._head = 0
        self._tail = 0
        self._data_count = 0
        # Track the amount of data that has passed through the buffer.
        self._read_count = 0
        self._write_count = 0
        self._cond = asyncio.Condition()
        self._closed = False

    @property
    def contents(self):
        """Return the state of the buffer.

        NOTE: This should not generally be called directly because the
        contents of the buffer are better managed by internal methods.
        """
        return self._buffer

    @property
    def buffsize(self):
        """Return the maximum size/amount of data the buffer can hold."""
        return self._buffsize

    @property
    def total_bytes_enqueued(self):
        """Return the total number of bytes pushed into the buffer.

        Useful to track the bytes "flowing into" the buffer.
        """
        return self._read_count

    @property
    def total_bytes_dequeued(self):
        """Return the total number of bytes popped out from the buffer.

        Useful to track the bytes "flowing out of" the buffer.
        """
        return self._write_count

    @property
    def current_count(self):
        """Return the number of bytes currently available in the buffer."""
        size = self._head - self._tail
        if size < 0:
            return size + self._buffsize
        return size

    async def close(self):
        """Mark this buffer as closing."""
        async with self._cond:
            self._closed = True
            self._cond.notify_all()

    async def byte_count_update(self):
        """Wait for the buffer to change.

        Returns
        -------
        2-tuple of: total_read, total_written
            Returns the current total number of bytes read and written.
        """
        async with self._cond:
            await self._cond.wait()
            return self._read_count, self._write_count

    async def enqueue(self, data):
        """Read/Enqueue data into the buffer.

        NOTE: This is not guaranteed to write all of the data into the buffer,
        but will instead return the number of bytes it was able to add at the
        time.
        """
        async with self._cond:
            count = 0
            while (not self._closed and self._data_count < self._buffsize and
                    count < len(data)):
                count += self._enqueue(data[count:])
            self._cond.notify_all()
            return count

    async def dequeue(self, buff):
        """Write/Dequeue data from the buffer.

        NOTE: This will read as much data as possible into the buffer, but
        no more.
        """
        async with self._cond:
            count = 0
            while self._data_count > 0 and count < len(buff):
                # Explicitly use a memoryview here to make sure we do not
                # assign incorrectly. 'view' is a window of the original
                # buffer.
                view = memoryview(buff)
                count += self._dequeue(view[count:])
            self._cond.notify_all()
            return count

    async def dequeue_bytes(self, count):
        buff = bytearray(count)
        await self.dequeue(buff)
        return buff

    async def clear(self):
        """Clear the buffer outright and treat it as empty."""
        async with self._cond:
            self._head = 0
            self._tail = 0
            self._data_count = 0
            # Notify that the buffer changed.
            self._cond.notify_all()

    def _enqueue(self, data):
        # Determine how much can be written into the buffer.
        to_queue = min(self._buffsize - self._data_count, len(data))
        if self._head >= self._tail:
            # Write starting from the head up until the end. We can write
            # up to the end of the buffer.
            offset = min(to_queue, self._buffsize - self._head)
        elif self._head < self._tail:
            offset = min(to_queue, self._tail - self._head)
        self._buffer[self._head:self._head + offset] = data[:offset]
        # Update the buffer head and handle wraparound.
        self._head += offset
        if self._head >= self._buffsize:
            self._head -= self._buffsize
        # Update various counters.
        self._data_count += offset
        self._read_count += offset
        return offset

    def _dequeue(self, buff):
        to_remove = min(len(buff), self._data_count)

        if self._head > self._tail:
            offset = min(to_remove, self._head - self._tail)
        else:
            offset = min(to_remove, self._buffsize - self._tail)
        buff[:offset] = self._buffer[self._tail:self._tail + offset]
        # Update the buffer tail and handle wraparound.
        self._tail += offset
        if self._tail >= self._buffsize:
            self._tail -= self._buffsize
        # Update various counters.
        self._data_count -= offset
        self._write_count += offset

        # Minor optimization: If '_data_count == 0' (no data is buffered),
        # reset 'head' to 0 to maximize the amount that can be written in
        # oneshot.
        if self._data_count == 0:
            self._head = 0
            self._tail = 0
        return offset
