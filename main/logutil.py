import contextlib
import logging
from logging.handlers import QueueHandler, QueueListener
import queue
import os.path
import os
import batchq


def create_queue_listener(memory:batchq.Memory, domain:str, *, format:str, loglevel:int, propagate:bool=True):

    key = 'log-file'

    if not memory.get_const(domain, key):
        return None

    formatter = logging.Formatter(format)

    logque = queue.Queue()
    queue_handler = QueueHandler(logque)
    queue_handler.setFormatter(formatter)

    logger = logging.getLogger(domain)
    logger.addHandler(queue_handler)
    logger.setLevel(loglevel)
    logger.propagate = propagate

    path = memory.get_path(logger.name, key)
    os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
    file_handler = logging.FileHandler(path, mode='w', encoding='utf-8', delay=True)

    return QueueListener(logque, file_handler)


@contextlib.contextmanager
def raii_app_logger(memory:batchq.Memory, loglevel):

    queue_listeners = None

    try:
        queue_listeners = (
            create_queue_listener(memory, 'batchq', loglevel=loglevel, format='%(levelname)s: %(pathname)s(%(lineno)d): %(process)d %(thread)d: %(message)s'),
            create_queue_listener(memory, 'batchq.utils.strmutil', loglevel=loglevel, format='%(message)s', propagate=False),
            create_queue_listener(memory, 'batchq.utils.queutil', loglevel=loglevel, format='%(message)s', propagate=False),
            create_queue_listener(memory, 'batchq.utils.inoutil', loglevel=loglevel, format='%(message)s', propagate=False),
        )

        queue_listeners = tuple(x for x in queue_listeners if x is not None)

        for queue_listener in queue_listeners:
            queue_listener.start()

        yield

    finally:
        if queue_listeners is not None:
            for queue_listener in queue_listeners:
                queue_listener.stop()


#