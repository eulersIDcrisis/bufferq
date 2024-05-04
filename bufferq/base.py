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
"""base.py.

Primary (Abstract) classes for bufferq.
"""
# Standard Library Imports
import abc
import time
import asyncio
import threading

try:
    diff_time = time.monotonic
except Exception:
    diff_time = time.time
# Local Imports
import bufferq.errors as errors

# Typing Imports
from collections.abc import Sequence, Collection, Iterator, AsyncIterator
from typing import Union, Optional, Any, TypeVar, Generic

Number = Union[int, float]
T = TypeVar("T")


# NOTE: In python 3.12+, there will be no need for this ``Generic[T]``
# subclassing since it supports generics intrinsically.
class AbstractQueue(abc.ABC, Generic[T]):
    """Queue class that supports a generator interface."""

    def __init__(self, maxsize: int = 0):
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
        with self._lock:
            self._empty_cond.notify_all()
            self._full_cond.notify_all()

    def reset(self):
        self._stop_event.clear()

    def push(self, item: T, timeout: Number = 0):
        """Put the given item onto the queue."""
        self.put_multi([item])

    def push_multi(self, items: Sequence[T], timeout: Number = 0):
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

    def pop(self, timeout: Optional[Number] = None) -> T:
        """Pop the next item from the queue."""
        items = self._pop_item_helper(1, timeout=timeout)
        return items[0]

    def pop_items(
        self, count: int = 1, timeout: Optional[Number] = None
    ) -> Sequence[T]:
        """Pop the next items in the queue.

        If count <= 0, this will pop all items in the queue. Otherwise, this
        will only pop UP TO 'count' items. (It _might_ pop that many elements,
        or it might pop less than that!)
        """
        return self._pop_item_helper(count, timeout)

    def pop_all(self, timeout: Optional[Number] = None) -> Sequence[T]:
        """Pop all of the items in the queue."""
        return self._pop_item_helper(-1, timeout=timeout)

    get = pop
    """Alias to pop()."""

    get_items = pop_items
    """Alias to pop_items()."""

    get_all = pop_all
    """Alias to pop_all()."""

    def consume_all_generator(self) -> Iterator[T]:
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

    def consume_items_generator(self, count: int = 1) -> Iterator[Sequence[T]]:
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

    def consume_one_generator(self) -> Iterator[T]:
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

    def empty(self) -> bool:
        """Return if the queue is empty.

        NOTE: This call is NOT thread-safe and so this result cannot be
        relied upon in to determine whether to add or remove items!

        Returns
        -------
        True if the queue is empty, False otherwise.
        """
        return self.qsize() == 0

    def full(self) -> bool:
        """Return if the queue is full.

        NOTE: This call is NOT thread-safe and so this result cannot be
        relied upon in to determine whether to add or remove items!

        Returns
        -------
        True if the queue is full, False otherwise.
        """
        if self.maxsize <= 0:
            return False
        return self.qsize() >= self.maxsize

    @abc.abstractmethod
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

    #
    # Helper Methods that manage the Condition Variables
    #
    def _push_item_helper(self, items: Collection[T], timeout: Optional[Number]):
        """Helper to push the given items to the queue.

        If this times out, it raises a QueueFull with the remaining items set
        on the exception.
        """
        # Store the end timestamp. If 'None', then there is no end timestamp.
        if timeout is None:
            end_ts = None
        elif timeout > 0:
            end_ts = diff_time() + timeout
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
                    elif diff_time() > end_ts:
                        raise
                    else:
                        # Wait for the remaining time.
                        wait_secs = min(30.0, end_ts - diff_time())
                    self._full_cond.wait(wait_secs)

    def _pop_item_helper(self, count: int, timeout: Optional[Number] = None):
        """Helper to pop up to 'count' items.

        If 'count <= 0', then return all items.

        This handles much of the boilerplate around the condition variable
        and timeouts when removing an item from the queue.
        """
        # Store the end timestamp. If 'None', then there is no end timestamp.
        if timeout is None:
            end_ts = None
        elif timeout > 0:
            end_ts = diff_time() + timeout
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
                    elif diff_time() > end_ts:
                        # We've timed out, so raise QueueEmpty as before.
                        raise
                    else:
                        # Wait for the remaining time or 30 seconds, whichever
                        # is smaller for good measure.
                        wait_secs = min(30.0, end_ts - diff_time())
                    self._empty_cond.wait(wait_secs)

    #
    # Required Overrideable Methods by Subclasses
    #
    @abc.abstractmethod
    def _push_item(self, item: T):
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

    @abc.abstractmethod
    def _pop_item(self) -> T:
        """Pop an item from the queue without blocking.

        If no item is available, this should raise `QueueEmpty`.

        Raises
        ------
        QueueEmpty:
            Raised if the queue is empty.
        QueueStopped:
            Raised if the queue is stopped.
        """

    #
    # Optionally Overrideable in Subclasses
    #
    def _push_items(self, items: Collection[T]):
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
            qf = errors.QueueFull("Queue is full!")
            qf.set_remaining_items(remaining_items)
            raise qf

    def _pop_items(self, max_count: int) -> Sequence[T]:
        """Return up to 'max_count' items from the queue without blocking.

        If no items are available, this should raise `QueueEmpty`.

        NOTE: This defines an abstraction used to handle different types of
        queues; subclasses should override this as appropriate. This should
        always return at least one item and no more than 'max_count' items or
        else raise 'QueueEmpty' if no items are currently available. The full
        implementation of this method should not need to acquire the local
        locks/condition variables for the queue.

        Parameters
        ----------
        max_count: int
            The maximum count to return.

        Returns
        -------
        list:
            List of items. This list will be non-empty with a maximum size
            equal to (or less than) 'max_count'

        Raises
        ------
        QueueEmpty: Raised when there are no items to return.
        """
        result = []
        for _ in range(max_count):
            try:
                result.append(self._pop_item())
            except errors.QueueEmpty:
                if len(result) > 0:
                    return result
                raise
        return result

    def _pop_all(self) -> Sequence[T]:
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
        result = []
        while True:
            try:
                result.append(self._pop_item())
            except errors.QueueEmpty:
                if result:
                    return result
                raise


#
# Asyncio Queues
#
class AsyncAbstractQueue(abc.ABC, Generic[T]):
    """An asynchronous Queue, similar to bufferq.Queue.

    This queue is designed to be used in an async context.
    """

    def __init__(self, maxsize: int = 0):
        self._maxsize = maxsize
        self._stopped = False
        self._lock = asyncio.Lock()
        self._empty_cond = asyncio.Condition(self._lock)
        self._full_cond = asyncio.Condition(self._lock)

    @property
    def maxsize(self) -> int:
        """Return the configured maximum size for this queue.

        If a nonpositive value is returned, the size is unlimited.
        """
        return self._maxsize

    async def stop(self) -> None:
        async with self._lock:
            self._stopped = True
            self._empty_cond.notify_all()
            self._full_cond.notify_all()

    async def push(self, item: Any, timeout: Number = 0):
        remaining = timeout
        start_ts = diff_time()
        async with self._full_cond:
            while not self._stopped:
                try:
                    self._push_item(item)
                    # Notify the empty condition that an item has been added.
                    self._empty_cond.notify()
                    return
                except errors.QueueFull as qf:
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
            raise errors.QueueStopped("Cannot add item to stopped queue.")

    async def pop(self, timeout: Number = 0) -> Any:
        remaining = timeout
        start_ts = diff_time()
        async with self._empty_cond:
            while not self._stopped:
                try:
                    item = self._pop_item()
                    # Notify the full condition that an item has been removed.
                    self._full_cond.notify()
                    return item
                except errors.QueueEmpty as qf:
                    # Exit the loop if there is nothing else to pop.
                    if self._stopped:
                        break
                    # Handle the timeout.
                    if remaining is None:
                        await self._empty_cond.wait()
                    elif remaining > 0:
                        try:
                            await asyncio.wait_for(self._empty_cond.wait(), remaining)
                        except asyncio.TimeoutError:
                            # Raise QueueEmpty instead of the timeout.
                            raise qf
                    else:
                        raise
            # Only get here if the queue is stopped AND empty.
            raise errors.QueueStopped()

    async def pop_all(self, timeout: Number = 0):
        """Pop all items in the queue, if any."""
        if timeout is None or timeout < 0:
            end_ts = None
        else:
            end_ts = diff_time() + timeout
        async with self._empty_cond:
            while not self._stopped:
                try:
                    return self._pop_all()
                except errors.QueueEmpty as qf:
                    if end_ts is None:
                        await self._empty_cond.wait()
                        continue
                    remaining = end_ts - diff_time()
                    if remaining <= 0:
                        raise
                    try:
                        await asyncio.wait_for(self._empty_cond.wait(), remaining)
                    except asyncio.TimeoutError:
                        raise qf
            # Only get here if the queue is stopped AND empty.
            raise errors.QueueStopped()

    async def consume_one_generator(self) -> AsyncIterator[T]:
        try:
            while True:
                item = await self.pop()
                yield item
        except errors.QueueStopped:
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
    def _push_item(self, item: T):
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
    def _pop_item(self) -> T:
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
    # Optional Overrides
    #
    def _pop_all(self) -> Sequence[T]:
        result = []
        try:
            while True:
                result.append(self._pop_item())
        except (errors.QueueEmpty, errors.QueueStopped):
            if result:
                return result
            raise
