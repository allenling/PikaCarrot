
import logging
import errno
import functools
from collections import deque
from types import coroutine
# maybe you want to use curio.Futureless, but since we are building a prototype, so it's ok to use futures.Future.
from concurrent.futures import Future

import pika
import pika.compat
from pika.adapters.select_connection import PollEvents

logger = logging.getLogger(__file__)



class PikaAsync:
    """
    see https://github.com/pika/pika/blob/main/examples/asynchronous_consumer_example.py
    """

    _consumer_tags = set()
    _cancelled_ctgs = set()
    closing = False

    def __init__(self, amqp_url):
        self._connection = pika.SelectConnection(parameters=pika.URLParameters(amqp_url),
                                                 on_open_callback=self.on_connection_open,
                                                 on_open_error_callback=self.on_connection_open_error,
                                                 on_close_callback=self.on_connection_closed,
                                                 )
        self._channel = None
        return

    def on_connection_open(self, _unused_connection):
        logger.info('Connection opened')
        self._connection.channel(on_open_callback=self.on_channel_open)
        return

    def on_connection_open_error(self, _unused_connection, err):
        logger.error('Connection open failed: %s', err)
        exit(-1)
        return

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        self._connection.ioloop.stop()
        return

    def on_channel_open(self, channel):
        logger.info('Channel opened')
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        # setup your app
        self._on_channel_open()
        return

    def _on_channel_open(self):
        # do your things
        raise NotImplementedError

    def on_channel_closed(self, channel, reason):
        logger.warning('Channel %i was closed: %s', channel, reason)
        self.close_connection()
        return

    def close_connection(self):
        if self._connection.is_closing or self._connection.is_closed:
            logger.info('Connection is closing or already closed')
            return
        self._connection.close()
        return

    def stop_consuming(self):
        self.closing = True
        if self._channel:
            logger.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            for ctag in self._consumer_tags:
                cb = functools.partial(self.on_cancelok, userdata=ctag)
                self._channel.basic_cancel(ctag, cb)
        return

    def on_cancelok(self, frame, userdata):
        logger.info('RabbitMQ acknowledged the cancellation of the consumer: %s', userdata)
        self._consumer_tags.remove(userdata)
        if not self._consumer_tags:
            self.on_stop_consuming()
        return

    def on_stop_consuming(self):
        # you can do your thing here after all consumers stopping consuming
        self.close()
        return

    def close(self):
        self._channel.close()
        return

    def run(self):
        self._connection.ioloop.start()
        return

# ======================= any kernel can implement the instructions below.


@coroutine
def kernel_trap(*request):
    result = yield request
    if isinstance(result, BaseException):
        raise result
    return result


async def await_future(fut):
    return await kernel_trap("_trap_future_wait", fut)


async def spawn(coro):
    return await kernel_trap('_trap_spawn', coro)


async def set_timeout(seconds):
    return await kernel_trap("_trap_set_timeout", seconds)


async def remove_timeout():
    return await kernel_trap("_trap_remove_timeout")


async def sleep(seconds):
    return await kernel_trap("_trap_sleep", seconds)


class TimeoutAfter:

    """
    no nested timeout support here for now!
    if you want to support nested timeout, please go and read Curio.
    """

    def __init__(self, seconds):
        self.seconds = seconds
        return

    async def __aenter__(self):
        await set_timeout(self.seconds)
        return

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await remove_timeout()
        return


TASK_STATE_INITED = "INITED"
TASK_STATE_READY = "READY"
TASK_STATE_WAITING = "WAITING"
TASK_STATE_RUNNING = "RUNNING"
TASK_STATE_STOPPED = "STOPPED"


class Task:
    _lastid = 1

    def __init__(self, coro):
        self.id = Task._lastid
        Task._lastid += 1
        self.name = None
        self.coro = coro
        self.state = TASK_STATE_INITED
        self.trap_result = None
        self.exception = None
        self.timeout_handler = None
        # run_count shows how many times this task has been executed
        self.run_count = 0
        return

    def __str__(self):
        return f"Task<{self.id}, {self.name if self.name else self.coro.__name__}, {self.state}>"

    def __repr__(self):
        return self.__str__()

    def set_exception(self, e):
        self.exception = e
        self.state = TASK_STATE_STOPPED
        return

    def set_running(self):
        # if this is stopped, it shouldn't be marked as running.
        if self.stopped:
            return
        self.state = TASK_STATE_RUNNING
        self.run_count += 1
        return

    def set_waiting(self):
        # if this is stopped, it shouldn't be marked as waiting.
        if self.stopped:
            return
        self.state = TASK_STATE_WAITING
        return

    def set_ready(self):
        # if this is stopped, it shouldn't be marked as ready.
        if self.stopped:
            return
        self.state = TASK_STATE_READY
        return

    @property
    def stopped(self):
        return self.state == TASK_STATE_STOPPED

    @property
    def fresh(self):
        return self.run_count == 0

    @property
    def timeout(self):
        return self.exception and isinstance(self.exception, TimeoutError)


# ===============================================


class Pikake(PikaAsync):
    """
    the kernel
    """

    _wake_queue = deque()
    _rdy_queue = deque()
    r_sock = None
    w_sock = None
    current_task = None
    running = False

    async def setup_app(self):
        raise NotImplementedError

    def _on_channel_open(self):
        # register our own fds for executing tasks
        # self.r_interrupt and self.w_interrupt are the equivalents of notify_sock and wait_sock in Curio.
        self.r_sock, self.w_sock = self._connection.ioloop._poller._get_interrupt_pair()
        self._connection.ioloop.add_handler(self.r_sock.fileno(), self.kernel_run, PollEvents.READ)
        self.running = True
        self.spawn_corofunc(self.setup_app)
        return

    def spawn_corofunc(self, corofunc, *args, **kwargs):
        # we have to manually call self._trap_spawn to start the coroutine when a channel opened.
        # we can not call self.spawn directly because we are not a purely, 100-percent coroutine-based event loop.
        self._trap_spawn(corofunc(*args, **kwargs))
        return

    def _trap_spawn(self, coro):
        # just wrap the coro into a Task, and append it into the rdy_queue, nothing else.
        task = Task(coro)
        self._rdy_queue.append(task)
        if self.current_task:
            # we will send the newly-spawn task back to the current task.
            # you can do something, like maintaining a list of tasks, or do nothing.
            self.current_task._trap_result = task
        else:
            # if we called _trap_spawn directly, then no current, we have to wake up the kernel here.
            self.w_sock.send(b'\x00')
        return task

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

    def _trap_set_timeout(self, seconds):
        logger.info(f"set timeout {seconds} for {self.current_task}")
        cb = functools.partial(self.on_timeout, task=self.current_task)
        self.current_task.timeout_handler = self._connection.ioloop.call_later(seconds, cb)
        return

    def on_timeout(self, task: Task):
        logger.info(f"{task} timeout!!")
        task.trap_result = TimeoutError()
        self.cb_and_rdy(task)
        return

    def _trap_remove_timeout(self):
        logger.info(f"remove timeout for {self.current_task}")
        self._connection.ioloop.remove_timeout(self.current_task.timeout_handler)
        self.current_task.timeout_handler = None
        return

    async def timeout_after(self, seconds, coro):
        async with TimeoutAfter(seconds):
            return await coro

    def _trap_sleep(self, seconds):
        logger.debug(f"run {self.current_task} in {seconds}s")
        cb = functools.partial(self.cb_and_rdy, task=self.current_task)
        self.current_task.set_waiting()
        self.current_task.timeout_handler = self._connection.ioloop.call_later(seconds, cb)
        self.current_task = None
        return

    def cb_and_rdy(self, task):
        self._rdy_queue.append(task)
        self.w_sock.send(b'\x00')
        return

    def kernel_run(self, fileno, events):
        try:
            self.r_sock.recv(512)
        except pika.compat.SOCKET_ERROR as err:
            if err.errno != errno.EAGAIN:
                raise
        if not self.running:
            logger.warning("we are not runnig, we are not going to execute any tasks")
            # here close all inited tasks by calling task.coro.close to
            # get rid of RuntimeWarning: coroutine was never awaited
            for task in self._rdy_queue:
                if task.fresh:
                    task.coro.close()
            return
        while len(self._wake_queue):
            # we do not wrap draining and iterating wake_queue in another seperate task awaiting on wait_sock as Curio does,
            # we just check the wake queue in one function just for simplicity.
            task, future = self._wake_queue.popleft()
            # see https://github.com/dabeaz/curio/blob/master/curio/kernel.py#L439
            if future and task.future is not future:
                continue
            task.future = None
            task.set_ready()
            self._rdy_queue.append(task)

        while len(self._rdy_queue):
            task = self._rdy_queue.popleft()
            self.current_task = task
            self.current_task.set_running()
            while self.current_task:
                try:
                    trap = self.current_task.coro.send(self.current_task.trap_result)
                except BaseException as e:
                    # If any exception has occurred, the task is done.
                    if isinstance(e, StopIteration):
                        self.current_task.result = e.value
                    else:
                        logger.error(f"{task} exception!!!!!!", exc_info=True)
                        self.current_task.set_exception(e)
                    self.current_task = None
                    break
                self.current_task.trap_result = None
                # we assume that there won't be any exception here
                getattr(self, trap[0])(*trap[1:])
        return

    def stop(self):
        # we can not just simply empty the ready_queue,
        # because there might be some threads appending tasks into the ready_queue.
        self.running = False  # we will not run any tasks by running as False.
        self.close()
        return

    # ===================================

    def acknowledge_message(self, delivery_tag):
        # no need to wrap this as a trap as it is asynchronous already.
        logger.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)
        return

    def nacknowledge_message(self, delivery_tag):
        logger.info('NAcknowledge_message message %s', delivery_tag)
        self._channel.basic_nack(delivery_tag, multiple=False, requeue=True)
        return

    async def declare_x(self, x_name, x_type, **x_params):
        fut = Future()
        self._channel.exchange_declare(exchange=x_name, exchange_type=x_type, callback=fut.set_result, **x_params)
        # we still await on this future
        await await_future(fut)
        logger.info(f"@@ declare_x done, {x_name}, {x_type}, {x_params}")
        return

    async def declare_q(self, q_name, **q_params):
        fut = Future()
        self._channel.queue_declare(queue=q_name, callback=fut.set_result, **q_params)
        # see the reason stated in method declare_x
        await await_future(fut)
        logger.info(f"@@ declare_q done, {q_name}, {q_params}")
        return

    async def bind_x_q(self, x_name, q_name, routing_key):
        fut = Future()
        self._channel.queue_bind(q_name, x_name, routing_key, callback=fut.set_result)
        await await_future(fut)
        logger.info(f"@@ bind_q done, {x_name}, {q_name}, {routing_key}")
        return

    async def consume_queue(self, q_name, **params):
        fut = Future()
        c_tag = self._channel.basic_consume(q_name, on_message_callback=self.spawn_on_msg, callback=fut.set_result, **params)
        self._consumer_tags.add(c_tag)
        await await_future(fut)
        logger.info(f"@@ consume_queue {q_name} done")
        return

    def spawn_on_msg(self, _unused_channel, basic_deliver, properties, body):
        # start the on_msg async function for you
        logger.debug('Received message \n# %s from %s: %s', basic_deliver.delivery_tag, properties.app_id, body)
        self.spawn_corofunc(self.on_msg, basic_deliver, properties, body)
        return

    async def on_msg(self, basic_deliver, properties, body):
        # do your things here
        raise NotImplementedError

    async def setup_qos(self, prefetch_count, global_qos=False):
        fut = Future()
        self._channel.basic_qos(prefetch_count=prefetch_count, global_qos=global_qos, callback=fut.set_result)
        await await_future(fut)
        logger.info(f"@@ setup_qos  done, {prefetch_count}, {global_qos}")
        return


def main():
    return


if __name__ == "__main__":
    main()
