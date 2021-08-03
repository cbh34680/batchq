import asyncinotify
import logging
import asyncio

from .. import *


__all__ = [
    'receive_event_until_local_end',
]

pkg_logger = logging.getLogger(__package__)
logger = logging.getLogger(__name__)
memory = get_memory()


async def receive_event_until_local_end(watchdir, mask, *, event_ready:asyncio.Event=None):

    local_end = memory.get_event('batchq', 'local-end')

    logger.info(f'WATCH({watchdir}) START MASK({mask})')

    with asyncinotify.Inotify() as inotify:
        watch = inotify.add_watch(watchdir, mask)

        if event_ready is not None:
            event_ready.set()

        async for event in inotify:

            logger.trace(f'EVENT({event.mask}): {event.path}')

            if local_end.is_set():
                break

            yield event

#