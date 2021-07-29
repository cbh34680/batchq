import typing
import logging
import json
import collections
import sys

from .. import *
from ..utils import *
from ..utils import jobutil
from ..utils import queutil


logger = logging.getLogger(__name__)
memory = get_memory()


async def handle_any(taskno:int, peername, arg_exec_params:typing.Dict):

    logger.warning(f'handle_any:{taskno}) {peername} {arg_exec_params}, NOOP')

    return None


async def handle_dump_memory(taskno:int, peername, arg_exec_params:typing.Dict):

    logger.debug(f'handle_dump_memory:{taskno}) {peername} {arg_exec_params}')

    return memory.raw()


async def handle_list_jobs(taskno:int, peername, arg_exec_params:typing.Dict):

    key = arg_exec_params['kwargs'].get('key', 'response')
    order_by = arg_exec_params['kwargs'].get('order-by', 'desc')
    limit = arg_exec_params['kwargs'].get('limit', sys.maxsize)

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


async def handle_show_job(taskno:int, peername, arg_exec_params:typing.Dict):

    job = arg_exec_params['args'][0]
    key = arg_exec_params['kwargs'].get('key', 'response')
    order_by = arg_exec_params['kwargs'].get('order-by', 'desc')
    limit = arg_exec_params['kwargs'].get('limit', sys.maxsize)

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


async def handle_be_master(taskno:int, peername, arg_exec_params:typing.Dict):

    logger.debug(f'handle_be_master:{taskno}) {peername} {arg_exec_params}')

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


async def handle_change_master(taskno:int, peername, arg_exec_params:typing.Dict):

    logger.debug(f'handle_change_master:{taskno}) {peername} {arg_exec_params}')

    peer_host = arg_exec_params['kwargs']

    new_master = '{}:{}'.format(peername[0], peer_host['listen-port'])

    await memory.helper.path_write('batchq.consumer', 'master-host', new_master)

    return None


async def handle_report(taskno:int, peername, arg_exec_params:typing.Dict):

    logger.debug(f'handle_report:{taskno}) {peername} {arg_exec_params}')

    kwargs = arg_exec_params['kwargs']
    exec_params = kwargs['exec-params']

    job = exec_params.get('job')
    if job is not None:
        await jobutil.save_data('response', peername, job, kwargs)

    logger.debug(f'peername={peername} job={job} kwargs={kwargs}')

    return None


async def handle_ping(taskno:int, peername, arg_exec_params:typing.Dict):

    logger.debug(f'handle_ping:{taskno}) {peername} {arg_exec_params}')

    curr_ts = current_timestamp()
    peeraddr = peername[0]

    peer_host = arg_exec_params['kwargs']
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


async def handle_pong(taskno:int, peername, arg_exec_params:typing.Dict):

    logger.debug(f'handle_pong:{taskno}) {peername} {arg_exec_params}')

    active_hosts = arg_exec_params['kwargs']['active-hosts']

    for host in active_hosts.values():
        host['peername'] = tuple(host['peername'])
        host['loadavg'] = tuple(host['loadavg'])

    memory.set_val('batchq.producer', 'active-hosts', active_hosts)

    return None

#