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


async def handle_pause(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.debug(f'handle_pause:{taskid}) {peername} {exec_params}')

    await memory.helper.path_write('batchq.consumer', 'pause', True)

    return None


async def handle_resume(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.debug(f'handle_resume:{taskid}) {peername} {exec_params}')

    await memory.helper.path_remove('batchq.consumer', 'pause')

    return None


async def handle_world_end(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.debug(f'handle_world_end:{taskid}) {peername} {exec_params}')

    active_hosts = memory.get_val('batchq.producer', 'active-hosts')

    for host in active_hosts.values():

        message = {
            'peername': host['peername'],
            'payload': {
                'worker-key': 'local-end',
            },
        }

        queue = memory.get_queue('batchq.postman')
        await queutil.put(queue, message, where=here())

        logger.info('propagate to peername={}'.format(str(host['peername'])))

    return None


async def handle_local_end(taskid:int, peername, worker:typing.Dict, exec_params:typing.Dict):

    logger.debug(f'handle_local_end:{taskid}) {peername} {exec_params}')

    logger.info(f'peer={peername}) receive local-end, shutdown local 1 sec later')
    loop = asyncio.get_running_loop()
    loop.call_later(1.0, local_end)

    return None

#