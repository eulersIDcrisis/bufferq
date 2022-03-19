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
"""errors.py.

Error classes for bufferq.
"""

class QueueError(Exception):
    """Base Exception for queues in bufferq."""


class QueueEmpty(QueueError):
    """Exception denoting an empty queue."""


class QueueFull(QueueError):
    """Exception denoting the queue is full (and not receiving items)."""

    def __init__(self, *args):
        super(QueueFull, self).__init__(*args)
        self._remaining_items = []

    def set_remaining_items(self, items):
        self._remaining_items = items

    @property
    def remaining_items(self):
        return self._remaining_items


class QueueStopped(QueueError):
    """Exception denoting that the queue has stopped."""
