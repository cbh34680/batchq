import typing
import logging
import json
import collections
import sys
import asyncio.streams

from .. import *
from ..utils import *
from ..utils import jobutil


logger = logging.getLogger(__name__)
memory = get_memory()


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

#