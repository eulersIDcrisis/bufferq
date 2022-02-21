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
"""async_buffer.py.

Module implementing some common (asynchronous) buffering utilities.
"""
import asyncio
from collections import deque
from bufferq.errors import QueueStopped, QueueFull


class AsyncQueue(object):
    """An asynchronous Queue, similar to bufferq.Queue.

    This queue is designed to be used in an async context.
    """

    def __init__(self, maxsize=None):
        self._cond = asyncio.Condition()
        self._maxsize = maxsize
        self._data = deque()
        self._stopped = False

    async def stop(self):
        async with self._cond:
            self._stopped = True
            self._cond.notify_all()

    async def push(self, item):
        async with self._cond:
            if self._stopped:
                raise QueueStopped('Cannot add item to stopped queue.')
            if self._maxsize and len(self._data) >= self._maxsize:
                raise QueueFull(
                    'Cannot add item to full queue (size: {})'.format(
                        self._maxsize))
            self._data.append(item)
            self._cond.notify()

    async def pop(self):
        async with self._cond:
            while True:
                if self._data:
                    return self._data.popleft()
                # Only raise this exception _after_ the queue is empty.
                if self._stopped:
                    raise QueueStopped()
                await self._cond.wait()

    async def consume_item_generator(self):
        try:
            while True:
                item = await self.pop()
                yield item
        except QueueStopped:
            return


class BufferedStream(object):
    """Queue-like stream of bytes for asynchronous contexts.

    This class is similar to a Queue but instead of popping individual
    bytes, this pops items from the buffer of the requested size in an
    optimized manner. Like with the 'bufferq.Queue', this supports the
    ability to drain the stream before closing.
    """

    def __init__(self):
        self._buffer = bytearray()
