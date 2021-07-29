import asyncio
import functools
import asyncinotify
import logging
import sys

from . import *
from .utils import *
from .utils import inoutil


__all__ = [
    'new_task_factory',
 ]

logger = logging.getLogger(__name__)
memory = get_memory()


def _cutarg(f):

    if iscoroutinefunction_or_partial(f):
        async def _f(*args, **kwargs):
            return await f()
    else:
        def _f(*args, **kwargs):
            return f()

    return _f


async def _main():

    num_tasks = memory.get_const('batchq.consumer', 'create-task')

    procmap = {
        'batchq': {
            'worker-config': {
                'modify': _cutarg(memory.reload_workers),
                'delete': _cutarg(memory.clear_workers),
            },
        },
        'batchq.consumer': {
            'softlimit': {
                'modify': functools.partial(memory.helper.load_path_val, defval=num_tasks, converter=int),
                'delete': memory.unset_val,
            },
            'master-host': {
                'modify': functools.partial(memory.helper.load_path_val, converter=peername_str2tuple),
                'delete': memory.unset_val,
            },
        },
        'batchq.netlistener': {
            'busy-threshold': {
                'modify': functools.partial(memory.helper.load_path_val, defval=sys.float_info.max, converter=float),
                'delete': memory.unset_val,
            },
        },
    }

    pathmap = { memory.get_path(k1, k2): (k1, k2, ) for k1, v1 in procmap.items() for k2 in v1.keys()  }

    watchdir = memory.get_path(__name__, 'watchdir')
    mask = asyncinotify.Mask.CLOSE_WRITE | asyncinotify.Mask.MOVED_TO | asyncinotify.Mask.MOVED_FROM | asyncinotify.Mask.DELETE | asyncinotify.Mask.ONLYDIR

    await async_os_makedirs(watchdir, mode=0o700, exist_ok=True)

    async for event in inoutil.receive_event_until_world_end(watchdir, mask):

        logger.trace(event)

        memory.helper.stats_incr(__name__, 'event')
        path_event = str(event.path)
        path_key = pathmap.get(path_event)

        if path_key is None:
            memory.helper.stats_incr(__name__, 'ignore')
            logger.warning(f'{path_event}: path_key is null, mask={event.mask}, skip')
            continue

        if event.mask & (asyncinotify.Mask.CLOSE_WRITE | asyncinotify.Mask.MOVED_TO):
            proc_key = 'modify'

        elif event.mask & (asyncinotify.Mask.DELETE | asyncinotify.Mask.MOVED_FROM):
            proc_key = 'delete'

        else:
            memory.helper.stats_incr(__name__, 'ignore')
            logger.warning(f'{path_event}: unknown mask={event.mask}, skip')
            continue

        logger.info(f'{proc_key}: {path_key}')

        memory.helper.stats_incr(__name__, proc_key)
        proc = procmap[path_key[0]][path_key[1]][proc_key]

        if iscoroutinefunction_or_partial(proc):
            await proc(path_key[0], path_key[1])

        else:
            proc(path_key[0], path_key[1])


class _MyTaskFactory(TaskFactory):

    async def main(self):
        try:
            await _main()

        except asyncio.CancelledError:
            logger.debug(f'catch cancel')

        except Exception as e:
            sys_exit(f'catch exception={type(e)}: {e}')

        else:
            logger.debug(f'main normal end')

    async def create_tasks(self):

        tasks = [
            asyncio.create_task(self.main(), name=f'{__name__}:cancellable:main-task'),
        ]

        return tasks


def new_task_factory():
    return _MyTaskFactory()

#