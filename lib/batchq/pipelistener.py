import asyncio
import aiofiles.os
import logging
import os.path

from . import *
from .utils import *
from .utils import strmutil
from .utils import textutil


__all__ = [
    'new_task_factory',
 ]

logger = logging.getLogger(__name__)
memory = get_memory()


async def on_client_connected(reader:asyncio.StreamReader, writer:asyncio.StreamWriter):

    peername = '@pipe'

    try:
        memory.helper.stats_incr(__name__, 'event')
        path = memory.get_path(__name__, 'output')

        async with aiofiles.open(path, mode='w') as fw:
            async for i, line in strmutil.readlines(reader, peername=peername, timeout=5.0, where=here()):

                try:
                    request = textutil.text2request(line)

                except Exception as e:
                    logger.warning(f'catch exception={type(e)}: {e}')

                    response = {
                        'code': 400,
                        'reason': type(e),
                        'description': str(e),
                    }

                else:
                    response = {
                        'code': 200,
                    }

                await fw.write(textutil.request2text(request) + '\n')
                await strmutil.writeline(writer, textutil.response2text(response), peername=peername, where=here())

                memory.helper.stats_incr(__name__, 'write')

            await strmutil.writeline(writer, strmutil.TERMINATER, peername=peername, where=here())

        logger.debug(f'done')

    except Exception as e:
        logger.warning(f'exception={type(e)}: {e}')

    finally:

        writer.close()


class _MyTaskFactory(TaskFactory):

    def __init__(self):

        self.server = None

    async def _main(self):

        await memory.get_event('batchq.splitter', 'ready').wait()

        path = memory.get_path(__name__, 'socket')

        try:
            await aiofiles.os.remove(path)
        except FileNotFoundError:
            pass

        path_dir = os.path.dirname(path)
        await async_os_makedirs(path_dir, mode=0o700, exist_ok=True)

        logger.info(f'listen socket-file = {path}')

        self.server = await asyncio.start_unix_server(on_client_connected, path=path)

        async with self.server:
            await self.server.serve_forever()

    async def main(self):

        try:
            await self._main()

        except asyncio.CancelledError:
            logger.debug(f'catch cancel')

        except Exception as e:
            sys_exit(f'catch {type(e)} exception={e}')

        else:
            logger.debug(f'main normal end')

        finally:
            memory.get_event(__name__, 'end').set()

    async def canceller(self):

        await memory.get_event(__package__, 'local-end').wait()

        logger.trace('fired canceller')

        if self.server is not None:
            logger.debug(f'close server')
            self.server.close()
            self.server = None

    async def create_tasks(self):

        tasks = [
            asyncio.create_task(self.canceller(), name=f'{__name__}:canceller-task'),
            asyncio.create_task(self.main(), name=f'{__name__}:main-task'),
        ]

        return tasks


def new_task_factory():
    return _MyTaskFactory()

#