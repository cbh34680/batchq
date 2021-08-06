import asyncio
import aiofiles
import aiofiles.os
import typing
import functools
import logging
import os.path
import collections

from . import *
from .utils import *
from .utils import queutil
from .utils import strmutil
from .utils import textutil


__all__ = [
    'new_task_factory',
 ]

logger = logging.getLogger(__name__)
memory = get_memory()


async def put_files_to_queue(queue:asyncio.Queue):

    path_params = { 'task;filename-suffix': 'dummy', }
    dummy_path = memory.get_path('batchq.splitter', 'valid', path_params=path_params)
    valid_dir = os.path.dirname(dummy_path)

    isdir = await async_os_path_isdir(valid_dir)

    num_put = 0

    if isdir:
        logger.debug(f'read from valid-dir={valid_dir}')

        for i, valid_path in enumerate(os.path.join(valid_dir, x) for x in os.listdir(valid_dir)):

            message = { 'path': valid_path, }
            logger.debug(f'{i}) put message={message}')

            await queutil.put(queue, message, where=here())

            num_put += 1

    else:
        logger.warning(f'{valid_dir}: is not dir')

    return num_put


async def query_each_requests(requests:typing.List, hosts:typing.List):

    hosts = sorted(hosts, key=functools.cmp_to_key(cmp_hostinfo))
    logger.trace(f'hosts={hosts}')

    pos = 0
    len_request = len(requests)

    for host in hosts:
        if pos >= len_request:
            break

        softlimit = host['softlimit']

        end = pos + min(10, softlimit)
        logger.debug(f'pos={pos} end={end} softlimit={softlimit}')

        lines = []
        host_requests = requests[pos: end]

        if not host_requests:
            break

        for request in host_requests:
            try:
                async with aiofiles.open(request['path'], mode='r') as fr:
                    async for line in fr:
                        lines.append(line.strip())

                        # one record only
                        break

            except FileNotFoundError as e:
                logger.warning(f'catch {type(e)} exception={e}, ignore')

        if lines:
            try:
                peername = host['peername']
                host['request-ts'] = current_timestamp()

                async for i, line in strmutil.readlines_after_writelines(peername, lines, timeout=5.0, where=here()):

                    request = host_requests[i]
                    response = textutil.text2response(line)

                    yield host, request, response

            except Exception as e:
                logger.warning(f'catch exception={type(e)}: {e}')

        pos += len(host_requests)

    if pos < len_request:
        for request in requests[pos:]:
            yield None, request, None


async def flush_requests(requests:typing.List, hosts:typing.List):

    retry_high = []
    retry_low = []

    async for host, request, response in query_each_requests(requests, hosts):

        logger.debug(f'host={host} request={request} response={response}')

        if response is None:
            retry_high.append(request)
            continue

        code = response['code']
        rename_key = None

        if code == 200:
            rename_key = 'sent'

        else:
            suggestion = response['retry-suggestion'].split(' ')

            if suggestion[0] == 'POSSIBLE':
                if current_timestamp() - request['takeout-ts'] < 7200:

                    if code == 503:
                        retry_high.append(request)

                    else:
                        retry_low.append(request)

                else:
                    rename_key = 'guveup'

            else:
                assert suggestion[0] == 'IMPOSSIBLE'
                rename_key = 'impossible'

        if rename_key:
            orig = request['path']
            filename = os.path.basename(orig)
            filename = '-'.join(filename.split('-')[1:])

            path_params = { 'task;filename': filename, }
            await memory.helper.path_rename(__name__, rename_key, orig=orig, path_params=path_params)

    return retry_high, retry_low


NextTimeouts = collections.namedtuple('NextTimeouts', ['new_record', 'exist_high', 'exist_low', 'not_decr', 'no_hosts'])

async def _main():

    requests = []

    queue = memory.get_queue(__name__)

    num_put = await put_files_to_queue(queue)
    logger.info(f'put {num_put} message to queue')

    TIMEOUTS = NextTimeouts(**{ k:v for k, v in zip(NextTimeouts._fields, memory.get_const(__name__, 'next-timeouts')) })
    logger.info(f'TIMEOUTS={TIMEOUTS}')

    timeout = None
    timeout_query = lambda: timeout

    async for i, message in queutil.get_message_until_eom(queue, timeout_query=timeout_query, where=here()):

        incr_key = None
        timeout = None

        if message is None:
            logger.trace(f'{i}) detect timeout')

            if requests:
                hosts = memory.get_val(__name__, 'active-hosts').values()

                if hosts:
                    prev_len = len(requests)

                    logger.trace(f'{i}) before request={len(requests)}')
                    retry_high, retry_low = await flush_requests(requests, hosts)
                    requests = retry_high + retry_low
                    logger.trace(f'{i}) after request={len(requests)}')

                    post_len = len(requests)

                    incr_key = 'flush'

                    if requests:
                        #timeout = 20.0 if prev_len == post_len else 10.0

                        if retry_high:
                            timeout = TIMEOUTS.exist_high

                        else:
                            timeout = TIMEOUTS.not_decr if prev_len == post_len else TIMEOUTS.exist_low

                else:
                    logger.warning(f'{i}) hosts is empty, remaining={len(requests)}')

                    incr_key = 'no-hosts'
                    timeout = TIMEOUTS.no_hosts

            else:
                incr_key = '********** no-works **********'
                logger.error(f'{i}) ********** no-works **********')
                assert False

        else:
            logger.debug(f'{i}) get message={message}')

            message.setdefault('takeout-ts', current_timestamp())
            requests.append(message)

            incr_key = 'append'
            timeout = TIMEOUTS.new_record

        memory.helper.stats_incr(__name__, incr_key)

        logger.debug(f'request={len(requests)}, next timeout={timeout}')

    async for i, message in queutil.get_message_until_empty(queue, where=here()):

        logger.debug(f'{i}) remaining valid-path={message}')

        memory.helper.stats_incr(__name__, 'remaining')


class _MyTaskFactory(TaskFactory):

    async def ainit(self):

        memory.set_val(__name__, 'active-hosts', {})

    async def main(self):
        try:
            await _main()

        except asyncio.CancelledError:
            logger.debug(f'catch cancel')

        except Exception as e:
            sys_exit(f'catch exception={type(e)}: {e}')

        else:
            logger.trace(f'main normal end')

    async def canceller(self):

        await memory.get_event(__package__, 'local-end').wait()

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