
# Pikake

an async/await distributed tasks application based on the top of the builtin event loop used in Pika

by this robust event loop,  we implement a asycio kernel as in Curio, which is a prominent educational python asyncio library.

and with this asyncio kernel, we build a distributed tasks celery-like application to simplify and streamline the entire process. 

## usage

an example of an Celery-like application. 

```python


from pikake import Pikake, spawn


class AsyncCelery(Pikake):

    async def setup_app(self):
        # after opening a channel successfully.
        self.spawn_workers()
        # here we request the kernel to spawn a task.
        await spawn(self.periodic_info())
        # ! maybe you want to track the task here.
        task = await spawn(self._setup())
        return

    async def _setup(self):
        await self.setup_qos(10, global_qos=True)
        # you might want to declare multiple exchanges and queues here.
        x_name, q_name, routing_key = "Pikake.x", "Pikake.q", "Pikake.k"
        await self.declare_x(x_name, "direct")
        await self.declare_q(q_name)
        await self.bind_x_q(x_name, q_name, routing_key)
        await self.consume_queue(q_name)
        return
app = AsyncCelery("amqp://guest:guest@localhost:5672/%2F")
app.start()
```

it's more of a prototype, or boilerplate shows how to build a Celery-like application with minimal functionalities.

you might want to add more features your own.

here we want to share the story of how we got here, and the problems we encountered, and a series of what, why and how.

so please bear with us.

## what we wanted.

in a nutshell, we needed a distributed task lib, just like Celery.

but Celery is kind of a little notorious for its implementation and its kind of messiness of the code.

to us, it's hard and laborious to go over its code to locate the lines of code that causes the problem.

but we do not want to disparage Celery, and actually we have been using Celery for a long time, and Celery is still being extensively
used in our system.

but we were open to a simpler alternative, and then we found dramatiq.

dramatiq is powerful, simple, and easy to use.

but there's one problem, it will open so many AMQP connections when you launch the app, basically one worker one connection.

as you can imagine, if you spawned 20 workers on a machine with 20 cores, you were going to have 20 AMQP connections to RabbitMQ,

and you would have 20 rows in the RabbitMQ management page just for one application, which is quite "no good" and unmanageable.

but we do not want to customize dramatiq since that's what dramatiq was designed to be. 

and dramatiq is still an excellent lib in terms of design and implementation, we are using it to connect to our Redis backend to run tasks.

and as continuing to explore other options, we found that there's a builtin event loop for us to use in pika!

## a notification system.

the first thing off the top of our head was to run that event loop in a sub-thread as a kind of notification service.

```

main thread <----> Futures for Notifications <-----> sub-thread(pika event loop)

```

why?

in most case you definitely do not want to do something to an underlying infrastructure like an event loop, and customization is going
to be the last resort.

we always assume that the responsibility of taking care of low-level details, like communicating with OS, is offloaded to a third party lib.

so we copied-pasted the code from https://github.com/pika/pika/blob/main/examples/asynchronous_publisher_example.py, and made some

small modifications for our case.

but a couple of problems arose.

### wake up in time!

ran the event loop in a sub-thread, and invoked that create_exchange method in main thread to declare an exchange, and when that
exchange was declared, an OK message would be sent back by RabbitMQ server.

but it took exactly 5 seconds to get the message and call our callback function, which did not make any sense.

because we ran the RabbitMQ server locally, and it shouldn't take so long to receive that exchange-declared-OK message.

but if we ran the event loop in the main thread, then everything was ok, we got that OK message instantly.

that fixed period of 5 seconds really got our attention, we guessed that must be some kind of timeout configuration.

and it turned out that the root cause was Windows Vs Linux, more precisely, Select Vs Epoll, as the default poller in Windows is Select,
and Epoll in Linux.

here we were invoking that create_exchange method to modify the fd as writable hoping that the event loop would wake up and send the
data immediately.

in Select, at least the implementation in pika, nothing to do when we modified the fd, meaning the event loop would not be
waked up by this "modification" operation.

```python

# pika/adapters/selection_connection.py

class SelectPoller(_PollerBase):
    def _modify_fd_events(self, fileno, events, events_to_clear, events_to_set):
        """The base class invoikes this method to notify the implementation to
        modify an already registered file descriptor. The request must be
        ignored if the poller is not activated.

        :param int fileno: The file descriptor
        :param int events: absolute events (READ, WRITE, ERROR)
        :param int events_to_clear: The events to clear (READ, WRITE, ERROR)
        :param int events_to_set: The events to set (READ, WRITE, ERROR)
        """
        # It's a no op in SelectPoller

```

so the event loop has to wait until the timeout, which is 5 seconds by default, to know that the fd is writable and send the data, if
there's no other events occur during that timeout period.

but in Epoll, there's a `epoll.modify` method to allow you to notify and wake up the event loop whenever you update a fd.

and the solution was straightforward, we had to wake up the event loop manually by sending a byte of data to the interrupt fd that Select

is listening on.

### got the response back even before registering your callback function successfully. 

another problem was about the expected order of the actions.

when you tried to send something, pika will first encode your data and put the encoded data in a buffer, and mark the fd as writable, and
when the event loop wakes up, it will read the bytes from the buffer, and send them.

and then the next step, it registers your callback function to the expected type of response message so that when the message back, pika

will look up the callback function by the message itself, and then invoke your callback.

to avoid the situation where we got the message back before we had done registering the callback function, we should first register the

callback function.

but this is not true in pika, and it seems that they got their reasons, see the comments.

```python

class Channel(object):

    def _rpc(self, method, callback=None, acceptable_replies=None):

        # Note: _send_method can throw exceptions if there are framing errors
        # or invalid data passed in. Call it here to prevent self._blocking
        # from being set if an exception is thrown. This also prevents
        # acceptable_replies registering callbacks when exceptions are thrown
        self._send_method(method)  # !here mark the fd as writable

        # If acceptable replies are set, add callbacks
        if acceptable_replies:
            # Block until a response frame is received for synchronous frames
            self._blocking = method.NAME
            LOGGER.debug(
                'Entering blocking state on frame %s; acceptable_replies=%r',
                method, acceptable_replies)

            # !code omitted here
            for reply in acceptable_replies:
                # !code omitted here
                # !here register your callback
                if callback is not None:
                    LOGGER.debug('Adding passed-in RPC response callback')
                    self.callbacks.add(self.channel_number,
                                       reply,
                                       callback,
                                       arguments=arguments)

```

this results in that the response might be back while we were still trying to register our callback in the main thread.

but can we get rid of this problem by just inverting the order of sending data and registering the callback in multithreading?

no, we can not.

because there's still a chance of the response received before the completion of callback registration.

you can not assume the execution order of main thread and the sub-thread, and thread switching is unpredictable.

### conclusion.

**in summary, there's a lack of across-thread, thread safety support in pika.**
> Looks like you're doing cross-thread channel/connection use which is not supported.
— https://github.com/pika/pika/issues/511

and the author prefers that users find their way to thread safety.

> Any plans on supporting cross-thread channel/connection?
> @ybaruchel no, not at this time. Pika is open-source so a motivated individual could contribute this feature, or build a library
on top of Pika that implements thread safety and other features.
— https://github.com/pika/pika/issues/511

## run the event loop in main thread.

as we can see, a multithreading architecture is not going to work for pika.

and running that event loop of pika in the main thread is definitely a good choice.

it's just a callback-based event loop, and all you have to do is register your callback for a certain event, and when that event

becomes available, your callback function will be invoked.

you do not have to worry problems we talked about above as there's just one thread.

it sounds easy and simple, and it's basically what people do in general, like a convention.

but alert, callback hell! and more importantly, we do not like to deal with those endless and come-from-nowhere callback chains.

is there anything else we can do?

## coroutines come to play.

coroutines are another option for us given that all tasks will be executed in workers that are running in either threads or processes,

and what main thread does is just waiting for some form of signals, e.g. events or future objects, to be set.

and the main thread will play a wait-and-launch, or wait-and-apply, wait-and-distribute role. 

and we love async/await and its nature of sequential executions in one async function.

```python

async def my_func1():
    await func1()
    await func2()
    return

async def my_func2():
    await func3()
    await func4()
    return

```

as you can see, the func2 runs after func1 finished, but you can not predict the execution order of func1 and func3.

and one more thing, there's exact one task running at a time in the context of scheduling, which makes it easy to test and
reason about compared to multithreading.

e.g. in some kind of extreme case where you may want to halt the system and stop running any tasks, this one-task inherent property
will be very helpful.

you can do that by just setting a global variable as false in one task, then by the time the kernel runs the next task, it
will first check the variable, and then just terminate and return.

you do not have to worry there's another "thread" modifying that global variable with the help of this one-task characteristic.

there's a couple of choices for us in terms of coroutines scheduling libs behind the scenes.

the first one, asyncio, which is included in Python as a standard lib, but complicated, and people tend to kind of replace it with
other libs.

and second Curio and Trio, both come from community and both have 4k+ stars on GitHub, and Trio is inspired by Curio according to its docs.

but the problem is that the tooling for those coroutine-based libs is immature, there's no amqp protocol implementation on top of them. 

adopting any of them means we have to implement amqp protocol ourselves based on them, which is very daunt, and unnecessary.

## why don't we just build our own async/awiat APIs?

the biggest challenge for building a coroutine-based lib is going to be a decent implementation of an event loop and, a solid
implementation of amqp protocol.

and fortunately we already have one, the pika per se!

and to implement our own coroutine-based lib `Pikake`, we tried to mimic Curio.

Curio is a prominent, concise, and neat coroutine-based lib in Python world.

> It(Curio) is meant to be both educational and practical.
— from GitHub ReadMe.

imitating Curio is a sensible choice if you want to implement your own lib. 

## what we did.

we decided to borrow the design from Curio, including a kernel, a list of traps, timeout and sleep features.

let's first break Curio down and show how it works.

### generators and coroutines

async/await keywords help you chain your coroutines, but there must be a generator at the end of the chain, more precisely, a
generator-based coroutine.

```
coro1 await <-> coro2 await <-> coro3 await <-> ... <-> generator-based yield

```

why?

because the yield keyword gives your function the ability to pause and resume, and more importantly, you can send something to the

outside world, and receive something from outside world via yield.

the yield works like a bridge between the caller and your coroutine chain.

for instance, you are waiting for data to arrive to a socket, you can yield that socket to someone and say if there's any data available

to read on this socket, please wake me up and hand it over to me.

at the mean time, when a coroutine is waiting for an IO event, and there might be another coroutine whose IO event becomes available, and

obviously, we should run it.

so we need someone somewhere being responsible for taking the socket and listening on that socket, and when data arrives, waking up the

coroutine by reading the data and sending it back to the coroutine.

this often conjures up images of thread switching, the OS kernel runs and pauses threads repeatedly and continually.

this is basically what we need, a simplified version of the OS kernel, a user mode kernel that runs at application level.

### the kernel and traps.

Curio implements more or less everything needed to build a kernel, let's introduce the those components and concepts involved.

first, there's a couple of states for a coroutine, waiting/sleeping, waked-up, ready, and running.

the waked-up coroutines are the coroutines whose IO event is available, and will be put into the wake queue, a coroutine that's ready

to run will be put into the ready queue, but a waked-up coroutine does not mean that it will be run at some point soon, there's

just some conditions we have to check before we run it.

if a waked-up coroutine is "good", then it will be put into the ready queue, let's just put the detail of this aside for now.

```

the wake_queue:  coro1, coro2, coro3

the ready_queue: coro4, coro5, coor6

```

to constantly run and pause coroutines, we keep popping coroutines from the ready queue and running them one by one, we repeat this in

a while true loop.

```python

while True:

    coro = ready_queue.popleft()
    run(coro)

```

in Python, to run a coroutine, we can iterate over it or send something into it, and when the coroutine pauses, it always yields
something out.

here what a coroutine yields is called a trap, a trap is an instruction that kernel can and should perform.

a coroutine yielding a trap means that it requests the kernel to do something for it, this is exactly equivalent to a thread invoking
a system call on the OS.

and the communication works as follows:

a coroutine yields a trap to the kernel, and the kernel executes the trap, and sends the result that the trap returns back to the coroutine.

```
the kernel                      the coroutine
                trap1 <--------  await trap1
             
execute the trap1

                the result1 -->  await trap1
                
                trap2 <--------  await trap2
             
execute the trap2

                the result2 -->  await trap2

```

sometimes a trap will cause the coroutine go to sleep, sometimes it will not, the trap might just tell the kernel to access some kind

of data and send it back to the coroutine.

the way the kernel runs can be summarized in one rule, it will execute traps until the coroutine goes to sleep.

but how do we know if a coroutine pauses? the solution is very simple.

before starting to run a coroutine, we will set it as the current task, and at any point of time, if the trap just marks the current

task as None, meaning that coroutine is sleeping.

here are lines of pseudocode, see the comments.

```python

while True:
    
    coro = ready_queue.popleft()
    current_task = coro

    while current_task:
        trap_name_and_args = current_task.send(current_task.trap_result)
        trap_func = trap_name_and_args[0]
        trap_args = trap_name_and_args[1:]
        # if the trap would not cause the current_task go to sleep, then it should do something like
        # current_task.trap_result = result.
        # otherwise, just set the current_task as None.
        trap_func(trap_args)

```

and also the traps and the kernel should be decoupled.

the traps are just specification, so that you can run your app on any kernel that implements those traps.

so Pikake is a kernel that implements a couple of traps.

e.g. there's a trap named future_wait, meaning the coroutine wants to wait on a future, and here is how we implemented that trap.

```python

# this is a trap
async def await_future(fut):
    return await kernel_trap("_trap_future_wait", fut)


class Pikake(PikaAsync):

    def future_back(self, fut: Future, task: Task):
        # the equivalent to the function wake in Curio.
        # put the task into the wake queue
        self._wake_queue.append((task, fut))
        # wake up kernel.
        self.w_sock.send(b'\x00')
        return

    def _trap_future_wait(self, fut: Future):
        # mark current task waiting
        task = self.current_task
        task.future = fut
        task.set_waiting()
        cb = functools.partial(self.future_back, task=task)
        fut.add_done_callback(cb)
        self.current_task = None
        return

```

### the timeout exception!

to us, one of the most elegant parts of Curio is going to be the function `_kernel_trap`.

```python

# This is the only entry point to the Curio kernel and the
# only place where the @types.coroutine decorator is used.
@coroutine
def _kernel_trap(*request):
    result = yield request
    if isinstance(result, BaseException):
        raise result
    else:
        return result
```

as we mentioned earlier, your coroutine chain must end with a generator based coroutine.

and this is the one and the only one generator based coroutine for all kernel traps in Curio.

this function makes it easy  and simple to raise exceptions for a coroutine.

suppose you were waiting on a future object, and you wanted to wait for 5 seconds at most. 

```python

try:
    self.timeout_after(5, await_future(fut))
except TimeoutError:
    # do something
    pass

```

as we can see, that `await_future` will invoke `_kernel_trap`, and `kernel_trap` does nothing except yielding everything passed in.

so that the kernel would execute `self._trap_future_wait` as `kernel_trap` yielded `_trap_future_wait` out.

let's just assume that the method `self.timeout_after` just awaits on `await_future`, we will talk about it later. 

if the future was still pending after 5 seconds, then the kernel would send a `TimeoutError` to the coroutine, then the `kernel_trap`

would rise that exception, and then we would catch that exception.

this is how we raise an exception in a coroutine, we send the exception into the coroutine!

without this `_kernel_trap`, a universal interface, you have to wrap all your traps as generator based coroutines, which is kind of 
redundant.

because the traps are doing basically the same thing, which is yielding the name of the trap and the parameters and raising the exception.

```python

@coroutine
def await_future(fut):
    res = yield "_trap_future_wait", fut
    if isinstance(res, BaseException):
        raise res
    return res

@coroutine
def sleep(n: float):
    res = yield "_trap_sleep", n
    if isinstance(res, BaseException):
        raise res
    return res

```

### timeout after x seconds.

but what the `self.timeout_after` does?

waiting for a period of time is another way of sleeping for a while.

if you want to wait a while for something to happen, basically you set an alarm to wake you up in like 5 seconds.

so before we await on `await_future`, `self.timeout_after` tells the kernel to set a timer to call this coroutine in 5 seconds by

yielding a trap named `set_timeout`.

setting and removing a timer requires maintaining a heap of items for an event loop, which is intricate.

and luckily, the event loop in pika has builtin support for this timer feature, it's easy to use.

```python

    def _trap_set_timeout(self, seconds):
        cb = functools.partial(self.on_timeout, task=self.current_task)
        self.current_task.timeout_handler = self._connection.ioloop.call_later(seconds, cb)
        return

    def on_timeout(self, task: Task):
        task.trap_result = TimeoutError()
        self.cb_and_rdy(task)
        return

    def cb_and_rdy(self, task):
        self._rdy_queue.append(task)
        self.w_sock.send(b'\x00')
        return
```

here we register a callback `self.on_timeout` to the event loop by calling `ioloop.call_later` method.

and as the name suggests, the event loop will call the `self.on_timeout` in 5 seconds.

and the `on_timeout` just assigns the task `TimeoutError`, and then puts the task into the ready queue, then wakes up the event loop.

this is how a `TimeoutError` exception being pushed into the coroutine when the operation takes longer than we expected to complete.

but what if the operation completed before timeout?

we would still get a `TimeoutError` exception as the `on_timeout` would be called in the future, even though we might have moved onto the

next block of code, which would be a fatal error. 

yes we would unless we remove the timer when the operation completes. 

so what `self.timeout_after` does is just two things, set a timer and remove a timer.

```python

try:
    self.timeout_after(5, await_future(fut))
except TimeoutError:
    # do something
    pass

```

this try-catch block above can be expanded as follows:

```python

async def set_timeout(seconds):
    return await kernel_trap("_trap_set_timeout", seconds)


async def remove_timeout():
    return await kernel_trap("_trap_remove_timeout")


try:
    await set_timeout(5)
    await await_future(fut)
except TimeoutError:
    # do something
    pass
else:
    await remove_timeout()

```
and here we can make use of a context manager to help us remove the timer implicitly.

```python
class TimeoutAfter:

    def __init__(self, seconds):
        self.seconds = seconds
        return

    async def __aenter__(self):
        await set_timeout(self.seconds)
        return

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await remove_timeout()
        return

```

`self.timeout_after` is going to call a `TimeoutAfter` object using keyword `async with`.

```python

    async def timeout_after(self, seconds, coro):
        async with TimeoutAfter(seconds):
            return await coro
```

and you might wonder what about nested `timeout_after` calls?

well here we do not support nested timeouts for now, but Curio does.

so please go and see Curio if you need nested timeouts feature.








