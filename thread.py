from concurrent.futures import ThreadPoolExecutor
from queue import Queue


class BoundedThreadPoolExecutor(ThreadPoolExecutor):
    """A wrapper around concurrent.futures.thread.py to add a bounded
    queue to ThreadPoolExecutor.
    """
    def __init__(self, *args, queue_size: int = 1000, **kwargs):
        """Construct a slightly modified ThreadPoolExecutor with a
        bounded queue for work. Causes submit() to block when full.
        Arguments:
            ThreadPoolExecutor {[type]} -- [description]
        """
        super().__init__(*args, **kwargs)
        self._work_queue = Queue(queue_size)
