import asyncio
import logging

from . import *
from .utils import *
from .utils import strmutil
from .utils import queutil
from .utils import textutil


__all__ = [
    'new_task_factory',
 ]

logger = logging.getLogger(__name__)
memory = get_memory()


async def _main():

    queue = memory.get_queue(__name__)

    async for i, message in queutil.get_message_until_eom(queue, where=here()):

        logger.debug(f'{i}) postman get message={message}')
        assert message is not None

        memory.helper.stats_incr(__name__, 'event')

        #peername = ('127.0.0.1', 9998)
        peername = message['peername']

        attempt = message.setdefault('attempt', 0)
        if attempt >= 4:

            logger.debug(f'guveup message={message}')
            await memory.helper.path_append_tsvln(__name__, 'guveup', message)
            memory.helper.stats_incr(__name__, peername, 'guveup')

            continue

        err = True

        try:
            text = json_dumps(message['payload'])

            async for _, line in strmutil.readlines_after_writelines(peername, text, timeout=5.0, where=here()):

                response = textutil.text2response(line)

                if response['code'] != 200:
                    raise Exception('code not 200, re-try')

                logger.debug(f'sent message={message}')
                await memory.helper.path_append_tsvln(__name__, 'sent', message)

        except Exception as e:
            logger.error(f'peername={peername} exception={type(e)}: {e}')

        else:
            err = False

            memory.helper.stats_incr(__name__, peername, 'sent')

        if err:
            attempt += 1
            message['attempt'] = attempt

            # re-try
            await asyncio.sleep(attempt)
            await queutil.put(queue, message, where=here())

            memory.helper.stats_incr(__name__, peername, 'attempt')

    async for i, message in queutil.get_message_until_empty(queue, where=here()):

        if message is None:
            continue

        peername = message['peername']

        logger.debug(f'guveup peername={peername} message={message}')
        await memory.helper.path_append_tsvln(__name__, 'guveup', message)
        memory.helper.stats_incr(__name__, peername, 'guveup')


class _MyTaskFactory(TaskFactory):

    async def main(self):
        try:
            await _main()

        except asyncio.CancelledError:
            sys_exit(f'catch cancel')

        except Exception as e:
            sys_exit(f'catch {type(e)} exception={e}')

        else:
            logger.trace(f'main normal end')

    async def canceller(self):

        await memory.get_event('batchq.consumer', 'end').wait()

        logger.trace('fired canceller')

        queue = memory.get_queue(__name__)
        await queutil.put_eom(queue, where=here())

    async def create_tasks(self):

        tasks = [
            asyncio.create_task(self.canceller(), name=f'{__name__}:canceller-task'),
            asyncio.create_task(self.main(), name=f'{__name__}:main-task'),
        ]

        return tasks


def new_task_factory():
    return _MyTaskFactory()

#