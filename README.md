# bufferq

Better Queue Interface for Python

Python's `queue` interface is quite clunky and really not that good.

`bufferq` is a separate queue implementation with a more intuitive interface.

## Sample Usage

Queues from `bufferq` are simple to use with a simple interface:
```python
import threading
import bufferq

q = bufferq.Queue()

def consumer(q):
    for item in q.consume_one_generator():
        print(f"Working on item: {item}")

thd = threading.Thread(target=consumer, args=(q,))
thd.daemon = True
thd.start()

q.put('a')
q.put('b')
time.sleep(1)
q.put_multi(list(range(5)))

q.stop()
thd.join()
```

## What's Wrong with `queue`?

Here are a few issues:

### Basic Operations

Python's `queue` interface is surprisingly overwhelming for simple tasks.

For example, adding/pushing items to the queue.
`queue.Queue.put()` has three different arguments:
 1. The item
 2. blocking (why?)
 3. timeout
(Options 2 and 3 are set so the operation blocks indefinitely until the
item can be added.)

This is annoying; why have `blocking` and `timeout` as separate arguments,
instead of simply letting `timeout=0` (or maybe even some placeholder-style
object if you are really, _REALLY_ concerned about blocking)? A `timeout=0`
should imply a single lookup that fails with `queue.Empty` if nothing is in
the queue without any additional arguments.
Yes, there is an added "convenience" call of `queue.Queue.put_nowait()`,
but this can just as easily be a proxy call to: `put(item, timeout=0)` which
can be added directly for clarity, but without muddying the rest of the
interface.

This same problem exists (and is more relevant) for the `get()` calls for
the queue.

### Design Issues

Python's queue also does not provide much help for common tasks that queues
are used for, such as a list of work for _Producer/Consumer_ design patterns.
Python's own `queue` documentation shows the following
[example](https://docs.python.org/3/library/queue.html#queue.Queue.join):
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
Even here in the official docs, there are problems. They've omitted the
necessary exception handling; if the worker were to raise an exception between
`q.get()` and `q.task_done()`, the call to `q.join()` might block indefinitely.
(Yes, `print()` is not likely to raise an exception, but real work done by such
a queue _is_...)
This can be fixed by adding try/finally, but the semantics are subtle and as
this example shows, error-prone.

The example also does not actually terminate the consumer thread correctly and
instead just lets it die as a daemon thread. This might be okay for an example,
but this is not good for realistic uses of the queue where resources need
stricter management. This is doubly ironic, because the point of `q.join()` is
to (presumably) support _draining_ the queue and block until the queue is
empty. However, any logic to handle basic draining requires more tooling that
is outside of the queue (i.e. checking some `threading.Event` instead of
`while True`), thus (in my opinion) defeating the point. This situation is
further complicated by the situation below:
```python
import queue

q = queue.Queue()
q.put('a')

def consumer():
    q.get()
    # Uh-oh. No timeout argument passed, so this blocks indefinitely.
    # Ctrl+C to get out of this, or worse (!) since some versions of python
    # did not even support Ctrl+C in this setting...
    q.get()

consumer()
```
The consumer might be blocked waiting for an element before it has a chance to
check whether the stop event was set.

### Better Design

The necessary variables to handle the draining should already implicitly be
available in the queue object, with improved calls. The queue should have some
`stop()` call that stops the queue and wakes up anyone waiting indefinitely to
insert/remove an item with a `QueueStopped()` exception or similar to avoid
deadlock.

Adding pythonic generators to remove items from the queue can also help with
these common cases; the consumer can simply iterate to obtain the next item
instead of handling the complicated `pop/get` logic that might otherwise be
required.

This is all provided by `bufferq.Queue` like below:
```python
import threading
import bufferq

q = bufferq.Queue()
def worker():
    for item in q.consume_one_generator():
        print(f'Working on {item}')
        print(f'Finished {item}')

# turn-on the worker thread
thd = threading.Thread(target=worker, daemon=True).start()

# send thirty task requests to the worker
for item in range(30):
    q.put(item)
print('All task requests sent, signal to stop and drain.')
# Request that the queue stop, since everything has been added.
q.stop()

thd.join()
print('All work completed and workers joined!')
```
