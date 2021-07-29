import asyncio
import itertools
import logging
import typing

from .. import *


__all__ = [
    'put_nowait',
    'get_message_until_eom',
    'EOM',
]

pkg_logger = logging.getLogger(__package__)
logger = logging.getLogger(__name__)


def put_nowait(queue:NamedQueue, arg:typing.Dict, *, where=None):

    queue.put_nowait(arg)
    qsize = queue.qsize()
    logger.trace(f'PUT(name={queue.name} size={qsize}): {where}: {arg}')


async def put(queue:NamedQueue, arg:typing.Dict, *, where=None):

    await queue.put(arg)
    qsize = queue.qsize()
    logger.trace(f'PUT(name={queue.name} size={qsize}): {where}: {arg}')


class EndOfMessage(object):
    def __repr__(self):
        return 'EOM'

EOM = EndOfMessage()

async def put_eom(queue:NamedQueue, **kwargs):

    return await put(queue, EOM, **kwargs)


async def get_message_until_eom(queue:NamedQueue, *, timeout_query=None, where=None):

    for i in itertools.count():
        try:
            timeout = None if timeout_query is None else timeout_query()
            message = await asyncio.wait_for(queue.get(), timeout=timeout)

        except asyncio.TimeoutError:
            logger.trace(f'TIMEOUT(name={queue.name}): {where}: timeout={timeout}')

            yield i, None

        else:
            queue.task_done()
            qsize = queue.qsize()

            if message is EOM:
                logger.trace(f'EOM(name={queue.name} size={qsize}): {where}')

                break

            logger.trace(f'GET(name={queue.name} size={qsize}): {where}: {message}')

            yield i, message


async def get_message_until_empty(queue:NamedQueue, *, where=None):

    try:
        for i in itertools.count():

            message = queue.get_nowait()
            queue.task_done()

            qsize = queue.qsize()
            logger.trace(f'GET(name={queue.name} size={qsize}): {where}: {message}')

            yield i, message

    except asyncio.QueueEmpty:
        logger.trace(f'EMPTY(name={queue.name}): {where}')

#