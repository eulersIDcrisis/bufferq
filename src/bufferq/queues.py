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
from typing import (
    Union, Iterable, Sequence, Optional, Any, Deque, List,
    Generator
)
# Standard Library Imports
import abc
import time
import heapq
import threading
from collections import deque
# Local imports
import bufferq.errors as errors

# Common typing.
Number = Union[int, float]


class QueueBase(metaclass=abc.ABCMeta):
    """Queue class that supports a generator interface."""

    def __init__(self, maxsize: int =0):
        self._maxsize = maxsize
        self._stop_event = threading.Event()
        # Define the lock explicitly here, in case subclasses or similar
        # want to define more condition variables on the same lock.
        self._lock = threading.RLock()

        # Use the same lock for both conditions.
        self._empty_cond = threading.Condition(self._lock)
        self._full_cond = threading.Condition(self._lock)

    @property
    def maxsize(self) -> int:
        """Return the configured maximum size for this queue.

        If non-positive, the size is presumed to be unlimited.
        """
        return self._maxsize

    def stop(self):
        """Stop the queue (but permit draining items still in it).

        Stopping the queue prevents items from being added to it until it is
        reset.
        """
        self._stop_event.set()
        # Not 100% sure if it is necessary to acquire the lock before notifying
        # the different condition variables.
        with self._lock:
            self._empty_cond.notify_all()
            self._full_cond.notify_all()

    def reset(self):
        self._stop_event.clear()
        pass

    def push(self, item: Any, timeout: Number =0):
        """Put the given item onto the queue."""
        self.put_multi([item])

    def push_multi(self, items, timeout: Number =0):
        """Put the given list of items onto the queue."""
        count = len(items)
        if count <= 0:
            # Nothing to push.
            return
        with self._full_cond:
            if self._stop_event.is_set():
                raise errors.QueueStopped()
            try:
                self._push_items(items)
            except errors.QueueFull as qf:
                # Only notify the number of items actually added.
                count -= len(qf.remaining_items)
                raise
            finally:
                self._empty_cond.notify(count)

    put = push
    """Alias to push()."""

    put_multi = push_multi
    """Alias to push_multi()."""

    def pop(self, timeout: Optional[Number] =None) -> Any:
        """Pop the next item from the queue."""
        items = self._pop_item_helper(1, timeout=timeout)
        return items[0]

    def pop_items(self, count: int =1, timeout: Optional[Number]=None) \
            -> Sequence[Any]:
        """Pop the next items in the queue.

        If count <= 0, this will pop all items in the queue. Otherwise, this
        will only pop UP TO 'count' items. (It _might_ pop that many elements,
        or it might pop less than that!)
        """
        return self._pop_item_helper(count, timeout)

    def pop_all(self, timeout: Optional[Number]=None) -> Sequence[Any]:
        """Pop all of the items in the queue."""
        return self._pop_item_helper(-1, timeout=timeout)

    get = pop
    """Alias to pop()."""

    get_items = pop_items
    """Alias to pop_items()."""

    get_all = pop_all
    """Alias to pop_all()."""

    def consume_all_generator(self) -> Generator[Sequence[Any], None, None]:
        """Return a generator that removes all items at each iteration.

        This iterator will block until items are available, then will yield
        all of them at each iteration.
        """
        while True:
            try:
                yield self.pop_all(timeout=None)
            except errors.QueueEmpty:
                continue
            except errors.QueueStopped:
                return

    def consume_items_generator(self, count: int =1) \
            -> Generator[Sequence[Any], None, None]:
        """Return a generator that removes 'count' items at each iteration.

        This iterator will block until items are available, then will yield
        _up to_ 'count' items in one iteration.
        """
        while True:
            try:
                yield self.pop_items(count, timeout=None)
            except errors.QueueEmpty:
                continue
            except errors.QueueStopped:
                return

    def consume_one_generator(self) -> Generator[Any, None, None]: 
        """Return a generator that consumes one item at a time from the queue.

        This iterator will block until items are available, then will yield
        them as appropriate. When the queue is stopped, this generator will
        quietly exit.
        """
        while True:
            try:
                yield self.pop()
            except errors.QueueEmpty:
                continue
            except errors.QueueStopped:
                return

    def qsize(self) -> int:
        """Return the number of elements in the queue.

        Subclasses should override this as appropriate.

        NOTE: This method is not _strictly_ required for most queue operations
        or details and generally should _not_ be relied upon. However, this is
        useful for inspecting and debugging. Subclasses should make a best
        effort to implement this, or else return -1 or similar to denote that
        this operation isn't supported (though for most queue types, this is
        easy to support...).
        """
        raise NotImplementedError('This queue does not implement qsize!')

    #
    # Helper Methods that manage the Condition Variables
    #
    def _push_item_helper(self, items: Any, timeout: Optional[Number]):
        """Helper to push the given items to the queue.

        If this times out, it raises a QueueFull with the remaining items set
        on the exception.
        """
        # Store the end timestamp. If 'None', then there is no end timestamp.
        if timeout is None:
            end_ts = None
        elif timeout > 0:
            end_ts = time.time() + timeout
        else:  # timeout <= 0
            # Guarantee that we only iterate over the loop once.
            end_ts = 0

        while not self._stop_event.is_set():
            with self._full_cond:
                try:
                    self._push_items(items)
                except errors.QueueFull as qf:
                    # Update the items to insert to be the remaining items.
                    items = qf.remaining_items
                    if end_ts is None:
                        wait_secs = 30.0
                    elif time.time() > end_ts:
                        raise
                    else:
                        # Wait for the remaining time.
                        wait_secs = min(30.0, end_ts - time.time())
                    self._full_cond.wait(wait_secs)

    def _pop_item_helper(self, count: int, timeout: Optional[Number]=None):
        """Helper to pop up to 'count' items.

        If 'count <= 0', then return all items.

        This handles much of the boilerplate around the condition variable
        and timeouts when removing an item from the queue.
        """
        # Store the end timestamp. If 'None', then there is no end timestamp.
        if timeout is None:
            end_ts = None
        elif timeout > 0:
            end_ts = time.time() + timeout
        else:  # timeout <= 0
            # Guarantee that we only iterate over the loop once.
            end_ts = 0

        while True:
            with self._empty_cond:
                try:
                    if count <= 0:
                        results = self._pop_all()
                    else:
                        results = self._pop_items(count)
                    # Notify up to the number of items that were removed.
                    self._full_cond.notify(len(results))

                    # Return the results after notifying _full_cond, since
                    # items were removed from the queue.
                    return results
                except errors.QueueEmpty:
                    if self._stop_event.is_set():
                        raise errors.QueueStopped()
                    if end_ts is None:
                        wait_secs = 30.0
                    elif time.time() > end_ts:
                        # We've timed out, so raise QueueEmpty as before.
                        raise
                    else:
                        # Wait for the remaining time or 30 seconds, whichever
                        # is smaller for good measure.
                        wait_secs = min(30.0, end_ts - time.time())
                    self._empty_cond.wait(wait_secs)

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


    #
    # Optionally Overrideable in Subclasses
    #
    def _push_items(self, items: Iterable[Any]):
        """Push the given items onto the queue without blocking.

        This should push as many items onto the queue as possible; when not
        all of the items can be pushed onto the queue, this should insert as
        many as possible, then raise a `QueueFull`, with the elements that
        could not be inserted attached to the exception.

        (If there is no bounds on the queue size, then no exception need be
        raised, of course.)

        This can be overridden in subclasses if there is a more efficient way
        to do this operation in bulk; otherwise, this falls back to calling
            `self._push_item()`
        individually for each item up until the items can be no longer added.
        """
        full = False
        remaining_items = []
        for item in items:
            try:
                if not full:
                    self._push_item(item)
                    continue
            except errors.QueueFull:
                full = True

            remaining_items.append(item)
        if remaining_items:
            qf = errors.QueueFull('Queue is full!')
            qf.set_remaining_items(remaining_items)
            raise qf

    def _pop_items(self, max_count: int):
        """Return up to 'max_count' items from the queue without blocking.

        If no items are available, this should raise `QueueEmpty`.

        NOTE: This defines an abstraction used to handle different types of
        queues; subclasses should override this as appropriate. This should
        always return at least one item and no more than 'max_count' items or
        else raise 'QueueEmpty' if no items are currently available. The full
        implementation of this method should not generally need to acquire the
        local locks/condition variables for the queue.

        Parameters
        ----------
        max_count: int
            The maximum count to return.

        Returns
        -------
        list:
            List of items. The length of this list MUST be greater than 0
            and less than or equal to 'max_count'

        Raises
        ------
        QueueEmpty: Raised when there are no items to return.
        """
        result = []
        for _ in range(max_count):
            try:
                result.append(self._pop_item())
            except errors.QueueEmpty:
                if result:
                    return result
                raise
        return result

    def _pop_all(self):
        """Return all items in the queue without blocking.

        If no items are available, this should raise `QueueEmpty`.

        NOTE: This defines an abstraction used to handle different types of
        queues; subclasses should override this as appropriate. This should
        always return the full contents of the queue and the queue should
        effectively be empty when exiting this call. If nothing was in the
        queue to begin with, this should raise 'QueueEmpty'.

        This call does _not_ guarantee any particular order of the elements
        for speed considerations; if the caller wants the elements sorted
        (i.e. as might be expected by a PriorityQueue), then it is the
        responsibility of the caller to do so.
        """
        result = deque()
        while True:
            try:
                result.append(self._pop_item())
            except errors.QueueEmpty:
                if result:
                    return result
                raise


#
# Implementations for Common Queue Types
#
class Queue(QueueBase):
    """Basic FIFO queue implementation."""

    def __init__(self, maxsize: int =0):
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


class PriorityQueue(QueueBase):
    """Priority Queue implementation.

    Internally manages items via a simple heap.
    """

    def __init__(self, maxsize: int =0):
        super(PriorityQueue, self).__init__(maxsize=maxsize)
        self._items: List[Any] = []

    def qsize(self) -> int:
        """Return the number of elements in the queue.

        NOTE: The result is _not_ thread-safe!
        """
        return len(self._items)

    def _push_item(self, item):
        if self.maxsize > 0 and self.maxsize > len(self._items):
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
