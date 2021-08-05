from functools import WRAPPER_ASSIGNMENTS
import typing
import logging
import json
import collections
import sys
import os
import asyncio
import asyncio.streams
import aiofiles

from .. import *
from ..utils import *
from ..utils import jobutil
from ..utils import queutil


logger = logging.getLogger(__name__)
memory = get_memory()


async def handle_any(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.warning(f'handle_any:{taskid}) {peername} {exec_params}')

    return None


async def handle_dump_memory(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.debug(f'handle_dump_memory:{taskid}) {peername} {exec_params}')

    return memory.raw()


async def handle_list_jobs(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.debug(f'handle_list_jobs:{taskid}) {peername} {exec_params}')

    key = exec_params['kwargs'].get('key', 'response')
    order_by = exec_params['kwargs'].get('order-by', 'desc')
    limit = exec_params['kwargs'].get('limit', sys.maxsize)

    buffer = collections.deque(maxlen=limit)

    async for cols in jobutil.each_history():
        if cols[3] != key:
            continue

        rec = {
            'datetime': cols[0],
            'peeraddr': cols[1],
            'job': cols[2],
        }

        if order_by == 'asc':
            buffer.append(rec)

            if len(buffer) >= limit:
                break

        else:
            buffer.appendleft(rec)

    ret = {
        'query': {
            'limit': limit,
            'order-by': order_by,
            'key': key,
        },
        'count': len(buffer),
        'records': tuple(buffer),
    }

    return ret


async def handle_show_job(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.debug(f'handle_show_job:{taskid}) {peername} {exec_params}')

    job = exec_params['args'][0]
    key = exec_params['kwargs'].get('key', 'response')
    order_by = exec_params['kwargs'].get('order-by', 'desc')
    limit = exec_params['kwargs'].get('limit', sys.maxsize)

    buffer = collections.deque(maxlen=limit)

    async for cols in jobutil.each_data(job, key=key):
        rec = {
            'datetime': cols[0],
            'peeraddr': cols[1],
            'data': json.loads(cols[2]),
        }

        if order_by == 'asc':
            buffer.append(rec)

            if len(buffer) >= limit:
                break

        else:
            buffer.appendleft(rec)

    ret = {
        'query': {
            'job': job,
            'limit': limit,
            'order-by': order_by,
            'key': key,
        },
        'count': len(buffer),
        'records': tuple(buffer),
    }

    return ret


async def handle_be_master(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.debug(f'handle_be_master:{taskid}) {peername} {exec_params}')

    listen_port = memory.get_const('batchq.netlistener', 'listen-port')
    active_hosts = memory.get_val('batchq.producer', 'active-hosts')

    for host in active_hosts.values():

        message = {
            'peername': host['peername'],
            'payload': {
                'worker-key': 'change-master',
                'exec-params': {
                    'kwargs': {
                        'listen-port': listen_port,
                    },
                },
            },
        }

        queue = memory.get_queue('batchq.postman')
        await queutil.put(queue, message, where=here())

    return None


async def handle_change_master(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.debug(f'handle_change_master:{taskid}) {peername} {exec_params}')

    peer_host = exec_params['kwargs']

    old_master = memory.get_val('batchq.consumer', 'master-host')
    new_master = '{}:{}'.format(peername[0], peer_host['listen-port'])
    logger.info(f'change master from={old_master} to={new_master}')

    await memory.helper.path_write('batchq.consumer', 'master-host', new_master)

    return None


async def handle_report(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.debug(f'handle_report:{taskid}) {peername} {exec_params}')

    kwargs = exec_params['kwargs']
    job = kwargs['exec-params'].get('job')

    if job is not None:
        await jobutil.save_data('response', peername, job, kwargs)

    logger.debug(f'peername={peername} job={job} kwargs={kwargs}')

    return None


async def handle_ping(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.debug(f'handle_ping:{taskid}) {peername} {exec_params}')

    curr_ts = current_timestamp()
    peeraddr = peername[0]

    peer_host = exec_params['kwargs']
    peer_hostid = peer_host['hostid']

    peer_host['peername'] = (peeraddr, peer_host.pop('listen-port'), )
    peer_host['loadavg'] = tuple(peer_host['loadavg'])

    active_hosts = memory.get_val('batchq.producer', 'active-hosts')
    active_hosts = { k: v for k, v in active_hosts.items() if curr_ts - v['ping-ts'] < 120 and v['hostid'] != peer_hostid }
    active_hosts[peeraddr] = peer_host
    logger.debug(f'active-hosts={active_hosts}')

    memory.set_val('batchq.producer', 'active-hosts', active_hosts)

    #
    # return pong
    #
    hostid = memory.get_val('batchq', 'hostid')

    if hostid != peer_hostid:

        message = {
            'peername': peer_host['peername'],
            'payload': {
                'worker-key': 'pong',
                'exec-params': {
                    'kwargs': {
                        'active-hosts': active_hosts,
                    },
                },
            },
        }

        queue = memory.get_queue('batchq.postman')
        await queutil.put(queue, message, where=here())

    return None


async def handle_pong(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.debug(f'handle_pong:{taskid}) {peername} {exec_params}')

    active_hosts = exec_params['kwargs']['active-hosts']

    for host in active_hosts.values():
        host['peername'] = tuple(host['peername'])
        host['loadavg'] = tuple(host['loadavg'])

    memory.set_val('batchq.producer', 'active-hosts', active_hosts)

    return None


async def handle_world_end(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.debug(f'handle_world_end:{taskid}) {peername} {exec_params}')

    if exec_params['kwargs'].get('propagate'):

        my_hostid = memory.get_val('batchq', 'hostid')
        #my_hostid = '***'

        active_hosts = memory.get_val('batchq.producer', 'active-hosts')
        other_hosts = ( v for v in active_hosts.values() if v['hostid'] != my_hostid )

        for host in other_hosts:

            message = {
                'peername': host['peername'],
                'payload': {
                    'worker-key': 'world-end',
                    'exec-params': {
                        'kwargs': {
                            'propagate': False,
                        },
                    },
                },
            }

            queue = memory.get_queue('batchq.postman')
            await queutil.put(queue, message, where=here())

            logger.info('propagate peername={}'.format(str(host['peername'])))

    logger.info(f'peer={peername}) receive world-end, shutdown local 1 sec later')
    loop = asyncio.get_running_loop()
    loop.call_later(1.0, local_end)

    return None


class PipeStream():

    def __init__(self, path):

        rpipe, wpipe = os.pipe()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)

        self.path = path
        self.rpipe = rpipe
        self.wpipe = wpipe
        self.reader = reader
        self.protocol = protocol

    async def __aenter__(self):

        loop = asyncio.get_running_loop()

        rfd = os.fdopen(self.rpipe, mode='r')
        transport, protocol = await loop.connect_read_pipe(lambda: self.protocol, rfd)

        self.rfd = rfd
        self.transport = transport
        self.protocol = protocol

        return self

    async def close(self):

        self.transport.close()

        if self.path is not None:

            async with await path_open(self.path) as f:
                while True:
                    line = await self.reader.readline()
                    if len(line) == 0:
                        break
                    await f.write(line.decode('utf-8'))

        os.close(self.wpipe)
        #os.close(self.rpipe) --> OSError: errno=9 'Bad Descriptor'
        self.rfd.close()

    async def __aexit__(self, *args, **kwargs):
        await self.close()
        return False


async def handle_subproc(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.debug(f'handle_subproc:{taskid}) {peername} {exec_params}')

    worker_key = worker['key']
    memory.helper.stats_incr(__name__, 'subproc', worker_key)

    returncode = -1
    proc = -1
    paths = None

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

        path_params = { 'task;taskid': taskid, }
        paths = { k: path_expand_placeholder(worker[k], path_params=path_params) if k in worker else None for k in ('cwd', 'stdout', 'stderr', ) }

        stdout = paths.get('stdout')
        stderr = paths.get('stderr')

        static_env = {
            'BQ_WORKER_KEY': worker_key,
            'BQ_CLIENT': peername[0],
            'BQ_SESSION': exec_params.get('job'),
            'BQ_TASKNO': taskid,
            'BQ_STDOUT': paths.get('stdout'),
            'BQ_STDERR': paths.get('stderr'),
        }

        if stdout:
            static_env['BQ_STDOUT'] = stdout
        if stderr:
            static_env['BQ_STDERR'] = stderr

        env = dict_deep_merge(exec_params['kwargs'], static_env)
        env = { k: expand_placeholder(str(v)) for k, v in env.items() if v is not None }

        async with PipeStream(stdout) as stdout, PipeStream(stderr) as stderr:

            conf = {
                'stdout': stdout.wpipe,
                'stderr': stderr.wpipe,
                'env': env,
                'cwd': paths.get('cwd'),
            }

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
        'paths' : paths,
    }

    return retval

#