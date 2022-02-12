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
import abc
import time
import heapq
import threading
from collections import deque


class QueueError(Exception):
    """Base Exception for queues in bufferq."""


class QueueEmpty(QueueError):
    """Exception denoting an empty queue."""


class QueueFull(QueueError):
    """Exception denoting the queue is full (and not receiving items)."""

    def __init__(self, remaining_items, *args):
        super(QueueFull, self).__init__(*args)
        self.remaining_items = remaining_items


class QueueStopped(QueueError):
    """Exception denoting that the queue has stopped."""


class SimpleQueueBase(metaclass=abc.ABCMeta):
    """Queue class that supports a generator interface."""

    def __init__(self):
        self._stopped = threading.Event()
        # Define the lock explicitly here, in case subclasses or similar
        # want to define more condition variables on the same lock.
        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)

    def push(self, item):
        """Put the given item onto the queue."""
        self.put_multi([item])

    def push_multi(self, items):
        """Put the given list of items onto the queue."""
        count = len(items)
        if count <= 0:
            raise QueueError('No items to push.')
        with self._cond:
            try:
                self._push_items(items)
            except QueueFull as qf:
                # Only notify the number of items actually added.
                count -= len(qf.remaining_items)
                raise
            finally:
                self._cond.notify(count)

    put = push
    """Alias to push()."""

    put_multi = push_multi
    """Alias to push_multi()."""

    def pop(self, timeout=0):
        """Pop the next item from the queue."""
        items = self._pop_item_helper(1, timeout=timeout)
        return items[0]

    def pop_items(self, count=1, timeout=0):
        """Pop the next items in the queue.

        If count <= 0, this will pop all items in the queue. Otherwise, this
        will only pop UP TO 'count' items. (It _might_ pop that many elements,
        or it might pop less than that!)
        """
        return self._pop_item_helper(count, timeout)

    def pop_all(self, timeout=0):
        """Pop all of the items in the queue."""
        return self._pop_item_helper(-1, timeout=timeout)

    get = pop
    """Alias to pop()."""

    get_items = pop_items
    """Alias to pop_items()."""

    get_all = pop_all
    """Alias to pop_all()."""

    def consume_all_generator(self):
        """Return a generator that removes all items at each iteration.

        This iterator will block until items are available, then will yield
        all of them at each iteration.
        """
        while not self._stopped.is_set():
            try:
                yield self.pop_all(timeout=None)
            except QueueEmpty:
                continue
            except QueueStopped:
                return

    def consume_items_generator(self, count=1):
        """Return a generator that removes 'count' items at each iteration.

        This iterator will block until items are available, then will yield
        _up to_ 'count' items in one iteration.
        """
        while not self._stopped.is_set():
            try:
                yield self.pop_items(count, timeout=None)
            except QueueEmpty:
                continue
            except QueueStopped:
                return

    def consume_one_generator(self):
        """Return a generator that consumes one item at a time from the queue.

        This iterator will block until items are available, then will yield
        them as appropriate. When the queue is stopped, this generator will
        quietly exit.
        """
        while not self._stopped.is_set():
            try:
                yield self.pop_item(timeout=None)
            except QueueEmpty:
                continue
            except QueueStopped:
                return

    def qsize(self):
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

    def _pop_item_helper(self, count, timeout):
        """Helper to pop up to 'count' items.

        If 'count <= 0', then return all items.

        This handles much of the boilerplate around the condition variable
        and timeouts when removing an item from the queue.
        """
        # Store the end timestamp. If 'None', then there is no end timestamp.
        if timeout is None:
            end_ts = None
        elif timeout == 0:
            # Guarantee that we only iterate over the loop once.
            end_ts = 0
        else:
            end_ts = time.time() + timeout

        while not self._stopped.is_set():
            with self._cond:
                try:
                    if count <= 0:
                        return self._pop_all()
                    return self._pop_items(count)
                except QueueEmpty:
                    if end_ts is None:
                        wait_secs = 30
                    elif time.time() > end_ts:
                        # We've timed out, so raise QueueEmpty as before.
                        raise
                    else:
                        # Wait for the remaining time or 30 seconds, whichever
                        # is smaller for good measure.
                        wait_secs = min(30, end_ts - time.time())
                    self._cond.wait(wait_secs)
        # If we exit the while-loop, then the queue was explicitly stopped.
        raise QueueStopped()

    @abc.abstractmethod
    def _push_items(self, items):
        """Push the given items onto the queue without blocking.

        This should push as many items onto the queue as possible; when not
        all of the items can be pushed onto the queue, this should insert as
        many as possible, then raise a `QueueFull`, with the elements that
        could not be inserted attached to the exception.

        (If there is no bounds on the queue size, then no exception need be
        raised, of course.)
        """
        pass

    @abc.abstractmethod
    def _pop_items(self, max_count):
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
        pass

    @abc.abstractmethod
    def _pop_all(self):
        """Return all items in the queue without blocking.

        If no items are available, this should raise `QueueEmpty`.

        NOTE: This defines an abstraction used to handle different types of
        queues; subclasses should override this as appropriate. This should
        always return at least one item and no more than 'max_count' items
        or else raise 'QueueEmpty' if no items are currently available.
        """
        pass


#
# Implementations for Common Queue Types
#
class SimpleQueue(SimpleQueueBase):
    """Simple queue implementation."""

    def __init__(self, maxsize=None):
        super(SimpleQueue, self).__init__()
        self._items = deque()
        self._maxsize = maxsize if maxsize else -1

    @property
    def maxsize(self):
        return self._maxsize

    def qsize(self):
        """Return the number of elements in the queue.

        NOTE: The result is _not_ thread-safe!
        """
        return len(self._items)

    def _push_items(self, items):
        if self._maxsize <= 0:
            self._items.extend(items)
            return
        available = self._maxsize - len(self._items)
        if len(items) <= insert_count:
            self._items.extend(items)
        elif available <= 0:
            raise QueueFull(items, "Queue is completely full!")
        else:
            # Insert the items that could be inserted, then raise QueueFull
            # with the remaining items. The items should be added from the
            # start, with the remainder raised in the exception.
            self._items.extend(items[0:available])
            raise QueueFull(
                items[available:],
                "Queue filled up, only {} items inserted.".format(available)
            )

    def _pop_items(self, max_count):
        result = []
        if not self._items:
            raise QueueEmpty()
        count = min(len(self._items), max_count)
        for i in range(count):
            result.append(self._items.popleft())

        # 'result' should never be empty.
        assert len(result) > 0, "Should have raise QueueEmpty..."
        return result

    def _pop_all(self):
        # Just swap out a new deque for speed.
        result = self._items
        self._items = deque()
        return result


class LIFOQueue(SimpleQueue):
    """A Last-In, First-out queue (i.e. a Stack)."""

    def _pop_items(self, max_count):
        result = []
        if not self._items:
            raise QueueEmpty()
        count = min(len(self._items), max_count)
        for i in range(count):
            result.append(self._items.pop())

        # 'result' should never be empty.
        assert len(result) > 0, "Should have raise QueueEmpty..."
        return result


class PriorityQueue(SimpleQueueBase):
    """Priority Queue implementation."""

    def __init__(self, maxsize=None):
        super(SimpleQueue, self).__init__()
        self._items = []
        self._maxsize = maxsize

    @property
    def maxsize(self):
        return self._maxsize

    def qsize(self):
        """Return the number of elements in the queue.

        NOTE: The result is _not_ thread-safe!
        """
        return len(self._items)

    def _push_items(self, items):
        if self._maxsize <= 0:
            self._items.extend(items)
            return
        available = self._maxsize - len(self._items)
        if len(items) <= insert_count:
            for item in items:
                heapq.heappush(self._items, item)
        elif available <= 0:
            raise QueueFull(items, "Queue is completely full!")
        else:
            # Insert the items that could be inserted, then raise QueueFull
            # with the remaining items. The items should be added from the
            # start, with the remainder raised in the exception.
            i = 0
            for item in itertools.islice(items, available):
                heapq.heappush(self._items, item)
            raise QueueFull(
                items[available:],
                "Queue filled up, only {} items inserted.".format(available)
            )

    def _pop_items(self, max_count):
        result = []
        if not self._items:
            raise QueueEmpty()
        count = min(len(self._items), max_count)
        while count > 0:
            count -= 1
            result.append(heapq.heappop(self._items))

        # 'result' should never be empty.
        assert len(result) > 0, "Should have raise QueueEmpty..."
        return result

    def _pop_all(self):
        # Just swap out the list.
        # TODO: Should the items actually be sorted? Most cases that pop
        # all of the elements might not strictly care about the exact order
        # of the full set of items once popped; they can sort them outside
        # of any locking.
        result = self._items
        self._items = []
        return result
