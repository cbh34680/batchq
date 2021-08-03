import platform
import asyncio
import itertools
import logging

from . import *
from .utils import *


__all__ = [
    'new_task_factory',
 ]

logger = logging.getLogger(__name__)
memory = get_memory()


async def on_timer(num_times=0):

    if platform.system() == 'Linux':

        converter = lambda arg: tuple( float(x) for x in arg.split()[0:3] )
        await memory.helper.load_path_val(__package__, 'loadavg', converter=converter)

    else:
        memory.set_val(__package__, 'loadavg', (0.0, 0.0, 0.0, ))

    #if num_times % 6 == 2:
    #    await memory.helper.path_println(__package__, 'last-shutdown', current_timestamp())
    #    await memory.helper.path_println(__package__, 'last-memory', memory)


async def _main():

    await memory.get_event('batchq.netlistener', 'ready').wait()

    for n in itertools.count(1):

        memory.helper.stats_incr(__name__, 'event')

        await memory.call_callback_coros(__name__, 'on-timer', num_times=n)

        if n % 6 == 2:
            logger.trace(f'\n\n********** dumps[{n}] ********** ==>\n{memory}\n********** dumps[{n}] ********** <==\n')

        await asyncio.sleep(10.0)


class _MyTaskFactory(TaskFactory):

    async def ainit(self):

        await on_timer()
        memory.append_callback_coro(__name__, 'on-timer', on_timer)

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