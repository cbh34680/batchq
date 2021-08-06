import asyncio
import aiofiles
import aiofiles.os
import logging
import asyncinotify

from . import *
from .utils import *
from .utils import inoutil
from .utils import queutil
from .utils import textutil


__all__ = [
    'new_task_factory',
 ]

logger = logging.getLogger(__name__)
memory = get_memory()


async def process_line(lineno, line, evno, event_name):

    line = line.strip()

    memory.helper.stats_incr(__name__, 'read')

    if not str_is_empty(line):
        try:
            textutil.text2request(line)

        except Exception as e:
            logger.warning(f'catch exception={type(e)}: {e}')

            path_key = 'invalid'
            filename_suffix = f'{evno}_{event_name}'

        else:
            path_key = 'valid'
            filename_suffix = f'{evno}-{lineno}_{event_name}'

        logger.trace(f'input is {path_key}')
        memory.helper.stats_incr(__name__, path_key)
        path_params = { 'task;filename-suffix': filename_suffix, }
        winfo = await memory.helper.path_println(__name__, path_key, line, mode='a', path_params=path_params)
        logger.debug(f'write [{path_key}] to {winfo}')

        if path_key == 'valid':

            message = { 'path': winfo[1], }
            logger.debug(f'put message={message}')

            queue = memory.get_queue('batchq.producer')
            await queutil.put(queue, message, where=here())

    else:
        memory.helper.stats_incr(__name__, 'empty')


async def _main():

    watchdir = memory.get_path(__name__, 'watchdir')
    await re_create_directory(watchdir)

    mask = asyncinotify.Mask.CLOSE_WRITE | asyncinotify.Mask.MOVED_TO | asyncinotify.Mask.ONLYDIR

    event_ready = memory.get_event(__name__, 'ready')

    evno = 0
    async for event in inoutil.receive_event_until_local_end(watchdir, mask, event_ready=event_ready):

        memory.helper.stats_incr(__name__, 'event')

        evpath = str(event.path)
        logger.trace(f'detected event: evpath={evpath}')

        lineno = 0

        try:
            async with aiofiles.open(evpath, mode='r') as fr:
                async for line in fr:
                    await process_line(lineno, line, evno, event.name)
                    lineno += 1

        except FileNotFoundError as e:
            logger.warning(f'catch {type(e)} exception={e}, ignore')

        else:
            await aiofiles.os.remove(evpath)

        evno += 1

    await async_shutil_rmtree(watchdir, ignore_errors=True)


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

    async def canceller(self):

        await memory.get_event('batchq.pipelistener', 'end').wait()

        logger.trace('fired canceller')

        for task in asyncio.all_tasks():
            task_name = task.get_name()

            if __name__ in task_name and 'main-task' in task_name:
                if not task.done():
                    task.cancel()
                    logger.trace(f'send cancel to {task_name}')

    async def create_tasks(self):

        tasks = [
            asyncio.create_task(self.canceller(), name=f'{__name__}:canceller-task'),
            asyncio.create_task(self.main(), name=f'{__name__}:main-task'),
        ]

        return tasks


def new_task_factory():
    return _MyTaskFactory()

#