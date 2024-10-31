
import time  # import this for thread workers
import queue
import logging
from concurrent.futures import Future
from collections import deque

from pika_corot import PikaCorot, await_future, spawn, TimeoutAfter, sleep
from threads import ThreadWorker, WorkItem, raise_thread_exception

logger = logging.getLogger(__file__)


class PikaCelery(PikaCorot):
    worker_id = 0
    workers_count = 10
    workers = dict()
    idle_workers = deque()

    def spawn_workers(self):
        # 10 thread workers.
        while len(self.workers) < self.workers_count:
            wname, wq = f"CreoWorker-{self.worker_id}", queue.Queue()
            t = ThreadWorker(wname, wq)
            self.workers[wname] = t
            t.start()
            self.idle_workers.append(wname)
            self.worker_id += 1
        return

    async def periodic_info(self):
        while self.running:
            await sleep(2)
            logger.info("this is a periodic string")
        logger.info("periodic_info task gone")
        return

    async def setup_app(self):
        # after opening a channel successfully.
        self.spawn_workers()
        # here we can call self.spawn to request the kernel to spawn a task.
        # ! maybe you want to track the task here.
        await spawn(self.periodic_info())
        task = await spawn(self._setup())
        return

    async def _setup(self):
        # you might want to declare multiple exchanges and queues here.
        await self.setup_qos(10, global_qos=True)
        x_name, q_name, routing_key = "PikaCelery.x", "PikaCelery.q", "PikaCelery.k"
        await self.declare_x(x_name, "direct")
        await self.declare_q(q_name)
        await self.bind_x_q(x_name, q_name, routing_key)
        await self.consume_queue(q_name)
        return

    async def on_msg(self, basic_deliver, properties, body):
        if self.closing:
            logger.warning(f"we are closing, so nack this message: \n{basic_deliver.delivery_tag}, \n{body}")
            self.nacknowledge_message(basic_deliver.delivery_tag)
            return
        n = int(body)
        fut = Future()
        self.spawn_workers()
        wname = self.idle_workers.popleft()
        t = self.workers[wname]
        work_item = WorkItem("time.sleep", fut, n)
        logger.info(f"dispatch {work_item} to worker {wname}")
        t.apply(work_item)
        self.current_task.name = "sleep_task"
        try:
            await self.timeout_after(10, await_future(fut))
            res = fut.result()
        except Exception as e:
            if isinstance(e, TimeoutError):
                logger.info(f"{work_item} timeout!!!")
                # timeout exception, we'll kill that thread
                raise_thread_exception(t.ident, TimeoutError)
                del self.workers[wname]
            else:
                logger.error(f"sleep {n} exception!", exc_info=True)
        else:
            self.idle_workers.append(wname)
            logger.info(f"sleep {n} seconds done, {work_item}, result {res}")
        self.acknowledge_message(basic_deliver.delivery_tag)
        return

    def on_stop_consuming(self):
        # we will not receive any message from RabbitMQ, so do something deconstruction.
        # spawn shutdown, and give other tasks a chance of proceeding, for instance, sending an ack.
        self.spawn_corofunc(self.shutdown)
        return

    async def shutdown(self):
        for name, t in self.workers.items():
            t.stop()
        try:
            async with TimeoutAfter(5):
                for name, t in self.workers.items():
                    await await_future(t.terminated_fut)
        except TimeoutError:
            pass
        alive_workers = []
        for name, t in self.workers.items():
            if t.is_alive():
                alive_workers.append(name)
        if alive_workers:
            logger.warning(f"workers {alive_workers} still alive, but we are leaving! the tasks will stay in the queue as unack.")
        self.stop()
        return

    def start(self):
        try:
            self.run()
        except KeyboardInterrupt:
            # we have to wait for 5 seconds to catch KeyboardInterrupt if there's no events to wake up the event loop.
            # because the event loop in pika is running at a timeout of 5 seconds!
            logger.info("KeyboardInterrupt!")
            pass
        self.stop_consuming()
        # wait for the event loop to be stopped eventually.
        self.run()
        logger.info("We Are Gone!")
        return


def main():
    logging.basicConfig(level=logging.INFO,
                        format="[%(asctime)s.%(msecs)03d]%(levelname)s %(process)d_%(thread)d[%(filename)s.%(funcName)s:%(lineno)d] %(message)s",
                        )
    x = PikaCelery("amqp://guest:guest@localhost:5672/%2F")
    x.start()
    return


if __name__ == "__main__":
    main()
