import typing
import logging
import asyncio
import asyncio.streams

from .. import *
from ..utils import *
from ..utils import jobutil
from ..utils import queutil


logger = logging.getLogger(__name__)
memory = get_memory()


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
    active_hosts = { k: v for k, v in active_hosts.items() if curr_ts - v['ping-ts'] < 45 and v['hostid'] != peer_hostid }
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


#