

import ctypes
import inspect
import queue
import logging
import threading
import datetime
import sys
from concurrent.futures import Future

logger = logging.getLogger(__file__)


class WorkItem:
    _last_id = 0

    def __init__(self, fn, fut: Future, *args, **kwargs):
        self.fn = fn
        self.id = WorkItem._last_id
        WorkItem._last_id += 1
        self.fut = fut
        self.args = args
        self.kwargs = kwargs
        self.started_at = None
        self.finished_at = None
        return

    def __str__(self):
        return f"WorkItem<{self.id}, {self.fn}, {self.args}, {self.kwargs}, {self.started_at}, {self.finished_at}>"

    def execute(self):
        self.started_at = datetime.datetime.now()
        info = self.fn.split(".")
        module = ".".join(info[:-1])
        fn_name = info[-1]
        res = None
        exc = None
        try:
            fn = getattr(sys.modules[module], fn_name)
            res = fn(*self.args, **self.kwargs)
        except Exception as e:
            if isinstance(e, TimeoutError):
                raise SystemExit
            exc = e
        self.finished_at = datetime.datetime.now()
        if exc:
            self.fut.set_exception(exc)
        else:
            self.fut.set_result(res)
        return


class ThreadWorker(threading.Thread):

    def __init__(self, worker_name, q):
        super(ThreadWorker, self).__init__(daemon=True)
        self.q = q
        self.name = worker_name
        self.terminated_fut = Future()
        return

    def apply(self, item: WorkItem):
        # you might want to delegate this to your kernel to let the kernel help you wait on this put
        # operation as what Curio does.
        self.q.put(item)
        return

    def run(self):
        logger.info(f"{self.name} is running")
        while True:
            try:
                work_item = self.q.get(timeout=0.1)
            except queue.Empty:
                continue
            if work_item is None:
                break
            try:
                work_item.execute()
            except SystemExit:
                logger.info(f"{self.name} is forced to exit!!!")
                exit(-1)
            logger.info(f"{self.name} done {work_item.fn} ")
        logger.info(f"{self.name} is terminated")
        self.terminated_fut.set_result(0)
        return

    def stop(self):
        # we can make this async/await in the future.
        self.q.put(None)
        return


def raise_thread_exception(thread_id, exception):
    exctype = (exception if inspect.isclass(exception) else type(exception)).__name__
    thread_id = ctypes.c_ulong(thread_id)
    exception = ctypes.py_object(exception)
    count = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, exception)
    if count == 0:
        logger.critical("Failed to set exception (%s) in thread %r.", exctype, thread_id.value)
    elif count > 1:  # pragma: no cover
        logger.critical("Exception (%s) was set in multiple threads.  Undoing...", exctype)
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, ctypes.c_long(0))
    return


def main():
    return


if __name__ == "__main__":
    main()

