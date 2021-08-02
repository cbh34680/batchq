import typing
import logging
import json
import collections
import sys
import asyncio

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

    new_master = '{}:{}'.format(peername[0], peer_host['listen-port'])

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
    active_hosts = { k: v for k, v in active_hosts.items() if curr_ts - v['ping-ts'] < 120 and peer_hostid != v['hostid'] }
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

        static_env = {
            'BQ_WORKER_KEY': worker_key,
            'BQ_CLIENT': peername[0],
            'BQ_SESSION': exec_params.get('job'),
            'BQ_TASKNO': taskid,
        }

        env = dict_deep_merge(exec_params['kwargs'], static_env)
        env = { k: expand_placeholder(str(v)) for k, v in env.items() if v is not None }

        path_params = { 'task;taskid': taskid, }
        paths = { k: path_expand_placeholder(worker[k], path_params=path_params) if k in worker else None for k in ('cwd', 'stdout', 'stderr', ) }

        conf = {
            'stdout': asyncio.subprocess.PIPE,
            'stderr': asyncio.subprocess.PIPE,
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

        for i, out_name in enumerate(('stdout', 'stderr', )):
            path = paths.get(out_name)

            if path is None:
                logger.debug(f'{taskid}) name={out_name} no-file')

            else:
                logger.trace(f'{taskid}) write name={out_name} file={path}')

                await path_write(path, outs[i].decode('utf-8'))

    except OSError as e:
        logger.error(f'{taskid}) err={e.errno} {e.strerror}')

        returncode = e.errno

    retval = {
        'returncode': returncode,
        'pid': pid,
        'paths' : paths,
    }

    return retval

#