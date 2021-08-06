import typing
import logging
import os
import asyncio
import asyncio.streams
import abc

from .. import get_memory
from ..utils import *


logger = logging.getLogger(__name__)
memory = get_memory()


async def handle_any(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.warning(f'handle_any:{taskid}) {peername} {exec_params}')

    return None

class PipeAdapter(abc.ABC):

    def __init__(self):

        rpipe, wpipe = os.pipe()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        rfd = os.fdopen(rpipe, mode='r')

        self.rpipe = rpipe
        self.wpipe = wpipe
        self.reader = reader
        self.protocol = protocol
        self.rfd = rfd

    async def __aenter__(self):

        loop = asyncio.get_running_loop()
        transport, _ = await loop.connect_read_pipe(lambda: self.protocol, self.rfd)

        self.transport = transport

        return self.wpipe

    @abc.abstractmethod
    async def flush(self): ...

    async def close(self):

        self.transport.close()

        await self.flush()

        os.close(self.wpipe)
        #os.close(self.rpipe) --> OSError: errno=9 'Bad Descriptor'
        self.rfd.close()

    async def __aexit__(self, *args, **kwargs):

        await self.close()
        return False

class FileAdapter(PipeAdapter):

    def __init__(self, config, *, path_params):

        self.path = path_expand_placeholder(config['path'], path_params=path_params)
        super().__init__()

    async def flush(self):

        async with await path_open(self.path) as f:

            while True:
                line = await self.reader.readline()
                if str_is_empty(line):
                    break

                await f.write(line.decode('utf-8'))

        logger.debug(f'flush path={self.path} done.')


class NoneAdapter(object):

    def __init__(self, *args, **kwargs): ...

    async def __aenter__(self):
        return None

    async def __aexit__(self, *args, **kwargs):
        return False


def create_adapter(config, path_params):
    try:
        adapter_name = str(config.get('adapter')).capitalize()
        klass = globals()[f'{adapter_name}Adapter']

        adapter = klass(config, path_params=path_params)

    except Exception as e:
        logger.warning(f'catch {type(e)} exception={e}, set default(NoneAdapter)')

        return NoneAdapter(config)

    else:
        return adapter


async def handle_subproc(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.debug(f'handle_subproc:{taskid}) {peername} {exec_params}')

    worker_key = worker['key']
    memory.helper.stats_incr(__name__, 'subproc', worker_key)

    returncode = -1
    proc = -1

    try:
        execbin = worker.get('execbin')
        if execbin is not None:
            execbin = expand_placeholder(execbin)

        args = [
            execbin,
            expand_placeholder(worker['program']),
        ]

        args += exec_params['args']
        args = [ str(v) for v in args if v is not None ]

        static_env = {
            'BQ_WORKER_KEY': worker_key,
            'BQ_CLIENT': peername[0],
            'BQ_SESSION': exec_params.get('job'),
            'BQ_TASKNO': taskid,
        }

        env = dict_deep_merge(exec_params['kwargs'], static_env)
        env = { k: expand_placeholder(str(v)) for k, v in env.items() if v is not None }

        path_params = { 'task;taskid': taskid, }
        cwd = path_expand_placeholder(worker.get('cwd'), path_params=path_params)

        stdout_adapter = create_adapter(worker.get('stdout'), path_params)
        stderr_adapter = create_adapter(worker.get('stderr'), path_params)

        async with stdout_adapter as stdout_wpipe, stderr_adapter as stderr_wpipe:

            conf = {
                'env': env,
                'cwd': cwd,
            }

            if stdout_wpipe is not None:
                conf['stdout'] = stdout_wpipe

            if stderr_wpipe is not None:
                conf['stderr'] = stderr_wpipe

            start_ts = current_timestamp()
            proc = await asyncio.create_subprocess_exec(*args, **conf)
            logger.debug(f'{taskid}) subprocess start pid={proc.pid}\nargs={args}')
            logger.trace(f'{taskid}) conf={conf}')

            outs = await proc.communicate()
            elapsed = current_timestamp() - start_ts
            returncode = proc.returncode
            pid = proc.pid

        logger.debug(f'{taskid}) subprocess end pid={pid} rc={returncode} elapsed={elapsed}')

    except OSError as e:
        logger.warning(f'{taskid}) err={e.errno} {e.strerror}')

        returncode = e.errno

    retval = {
        'returncode': returncode,
        'pid': pid,
    }

    return retval

#