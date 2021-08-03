import json
import asyncio
import typing
import logging
import sys

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

_EMPTY_REQUEST = {
    'exec-params': {
        'args': [],
        'kwargs': {},
    },
}


async def build_response(line:str, *, threshold:float, loadavg:typing.Optional[tuple]):

    response = {
        'code': 200,
    }

    if loadavg is not None:
        if loadavg[0] > threshold:
            response['code'] = 503
            response['reason'] = 'SERVER BUSY'
            response['retry-suggestion'] = 'POSSIBLE LATER'
            return None, response

    else:
        logger.warning(f'not check loadavg={loadavg} threshold={threshold}')

    try:
        request = textutil.text2request(line)

    except json.JSONDecodeError:
        response['code'] = 400
        response['reason'] = 'DECODE FAULT'
        response['retry-suggestion'] = 'IMPOSSIBLE ANYTIME'
        return None, response

    except KeyError:
        response['code'] = 400
        response['reason'] = 'NO NAME'
        response['retry-suggestion'] = 'IMPOSSIBLE ANYTIME'
        return None, response

    except Exception as e:
        response['code'] = 400
        response['reason'] = f'BAD VALUE: {e}'
        response['retry-suggestion'] = 'IMPOSSIBLE ANYTIME'
        return None, response

    request = dict_deep_merge(_EMPTY_REQUEST, request)

    returns = await memory.call_callback_coros(__name__, 'on-check-request', request, response)

    if not all(returns):
        return None, response

    # all True
    return request, response


async def on_client_connected(reader:asyncio.StreamReader, writer:asyncio.StreamWriter):

    peername = writer.get_extra_info('peername')

    try:
        peeraddr = peername[0]

        loadavg = memory.get_val(__package__, 'loadavg')

        threshold = memory.get_val(__name__, 'busy-threshold')
        queue = memory.get_queue('batchq.consumer')

        memory.helper.stats_incr(__name__, 'event')
        memory.helper.stats_incr(__name__, peeraddr, 'call')

        async for i, line in strmutil.readlines(reader, peername=peername, timeout=5.0, where=here()):

            request, response = await build_response(line, threshold=threshold, loadavg=loadavg)

            if request is None:
                path_key = 'reject'

                logger.warning(f'reject line={line} response={response}')

            else:
                path_key = 'accept'

                message = {
                    'peername': peername,
                    'request': request,
                }

                predicate = lambda x: bool(x)
                request_done = await memory.call_callback_coros_until_if(__name__, 'on-exec-request', message, response, predicate=predicate)

                if request_done:
                    logger.debug(f'request line={line} proceeded')
                    path_key = 'proceed'

                else:
                    logger.debug(f'put(to consumer) message={message} ')
                    queutil.put_nowait(queue, message, where=here())

            response = textutil.response2text(response)
            await strmutil.writeline(writer, response, peername=peername, where=here())

            await memory.helper.path_append_tsvln(__name__, path_key, peeraddr, line, response)

            memory.helper.stats_incr(__name__, path_key)
            memory.helper.stats_incr(__name__, peeraddr, path_key)

        await strmutil.writeline(writer, strmutil.TERMINATER, peername=peername, where=here())

    except Exception as e:
        logger.warning(f'peername={peername} exception={type(e)}: {e}')

    finally:
        writer.close()


class _MyTaskFactory(TaskFactory):

    def __init__(self):

        self.server = None

    async def ainit(self):

        hook = lambda: memory.set_val(__name__, 'busy-threshold', sys.float_info.max)
        memory.append_val_hook(__name__, 'busy-threshold', 'unset_val', hook=hook)
        await memory.helper.load_path_val(__name__, 'busy-threshold', defval=sys.float_info.max, converter=float)

    async def _main(self):

        listen_port = memory.get_const(__name__, 'listen-port')
        logger.info(f'listen port = {listen_port}')

        self.server = await asyncio.start_server(on_client_connected, '0.0.0.0', listen_port)

        async with self.server:

            memory.get_event(__name__, 'ready').set()
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