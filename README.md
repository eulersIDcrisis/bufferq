# bufferq

Better Queue Interface for Python

Python's `queue` interface is really not that good and is quite clunky.

As such, I wrote this as a library of common buffer and queue patterns with a
more intuitive interface.

## Sample Usage

Queues from `bufferq` are simple to use with a simple interface:
```python
q = bufferq.SimpleQueue()

q.put('a')
q.put('b')

```

## What's Wrong with `queue`?

There are various problems with python's `queue`, but here are the main issues
for me.

### Basic Operations

One problem is that the interface is surprisingly overwhelming to accomplish
simple tasks. Like for example, adding/pushing items to the queue. The call to
`queue.Queue.put()` has three different arguments:
 1. The item
 2. blocking (why?)
 3. timeout

By default, (2) and (3) are set so that the operation blocks indefinitely
until the item can be added.

This interface is already somewhat annoying; would it not be easier to only
have two arguments, the `item` and the timeout, and let a `timeout=0` imply
not to block? Sure, you might argue that there is a difference between
`timeout=0` and `timeout=1e-7` or some such thing, but a plain `0` should be
interpreted as an integer in all cases. If you care enough about the difference
between `0` and `1e-7`, you can explicitly make the call with an integer.
Even a custom object type would be preferable, IMO, than having another
positional argument.

There is also this added `put_nowait()`; this can explicitly make the right
call necessary by properly aliasing to `put()`, but then why bother having
this extra argument to `put()` in the first place?

All of these same issues likewise apply to the more relevant `get()` and
`get_nowait()` calls of the queue.

### Design Issues

Python's queue also suffers from usability problems beyond direct calls.
For example, a common use is a _Producer/Consumer_ situation with various
producers adding items to the queue and consumers consuming them. Python's
own `queue` documentation shows the following [example](https://docs.python.org/3/library/queue.html#queue.Queue.join):
```python
import threading, queue

q = queue.Queue()

def worker():
    while True:
        item = q.get()
        print(f'Working on {item}')
        print(f'Finished {item}')
        q.task_done()

# turn-on the worker thread
threading.Thread(target=worker, daemon=True).start()

# send thirty task requests to the worker
for item in range(30):
    q.put(item)
print('All task requests sent\n', end='')

# block until all tasks are done
q.join()
print('All work completed')
```
However, this example is already problematic. Even here in the official docs,
they have omitted the necessary exception handling. If the worker were to raise
an exception between `q.get()` and `q.task_done()`, the call to `q.join()` will
block indefinitely. The exception-handling could be fixed by properly using a
try/finally block, but the semantics here are subtle and error-prone.

That's not the only problem. The point of `q.join()` is to presumably support
_draining_ the queue; however, again in this example, there is no logic that
tells the consumers when to stop. A simple fix in the example is to replace
`while True` with `while not stop_event.is_set()` or similar then set this
when the consumers should drain. But then there is no guarantee that all items
will be drained from the queue, again causing a deadlock around `q.join()`.
There is also risk that items are added indefinitely into the queue quickly
enough to prevent `q.join()` from ever reducing to 0. However it happens, the
point is that there are many different pitfalls with no easy answers built in
the `queue` library itself.

There is also the risk of code like:
```python
import queue

q = queue.Queue()
q.put('a')

q.get()
# Uh-oh. No timeout argument passed, so this blocks indefinitely.
# Ctrl+C to get out of this, or worse (!) since some versions of python
# did not even support Ctrl+C in this setting...
q.get()
```
Realistically, queues should have a way of notifying that they are done so that
code like this would eventually exit without the need for drastic measure.

### Better Design

A better design, in my opinion, is not to join with the queue, but the worker
threads themselves and simply notify when the queue is shutting down. In the
bad case of calling `q.get()` too many times, stopping the queue would notify
such a thread by raising `QueueEmpty` when the queue is empty and explicitly
stopped.

Fundamentally, it is better to join with the worker threads directly because
they could crash and they aren't truly done until they explicitly exit.
(Imagine if the thread was not declared as a `daemon` thread, for example.) It
is all around better to mark the work as done when:
 - Someone indicates that it is okay to stop.
 - There are no more workers.

`bufferq.SimpleQueue` and its variety of subclasses solve this with the (much)
more natural approach above; instead of managing the loop directly, it uses
generators and adds a `stop()` method to the queue to flag that the queue
should drain; it also flags that the queue should no longer accept any elements.

The same example above using `bufferq` might look like:
```python
import threading
import bufferq

q = bufferq.SimpleQueue()
def worker():
	for item in q.consume_one_generator():
        print(f'Working on {item}')
        print(f'Finished {item}')

# turn-on the worker thread
thd = threading.Thread(target=worker, daemon=True).start()

# send thirty task requests to the worker
for item in range(30):
    q.put(item)
print('All task requests sent\n', end='')
# Request that the queue stop, since everything has been added.
q.stop()

thd.join()
print('All work completed and workers joined!')
```
