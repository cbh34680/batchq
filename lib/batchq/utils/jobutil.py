import logging
import os
import binascii
import aiofiles

from .. import *
from . import *
from . import textutil


__all__ = [
    'save_data',
    'each_history',
    'each_data',
]

logger = logging.getLogger(__name__)
memory = get_memory()


def job2subdir(arg:str):

    arg2c = arg[0:2]
    x16str = binascii.hexlify(arg2c.encode()).decode()

    subdir = os.path.join(x16str[0:2], x16str[2:4], arg2c, arg[len(arg2c):])

    return subdir


async def save_data(key, peername, job, data):

    textutil.validation_job(job)

    subdir = job2subdir(job)

    await memory.helper.path_append_tsvln(__name__, 'history', peername[0], job, key, subdir)

    path_params = { 'task;job-subdir': subdir, }
    await memory.helper.path_append_tsvln(__name__, key, peername[0], json_dumps(data), path_params=path_params)


async def each_history():

    path = memory.get_path(__name__, 'history')

    async with aiofiles.open(path, mode='r') as fr:
        async for line in fr:
            yield line.strip().split('\t')


async def each_data(job, *, key):

    subdir = job2subdir(job)

    path_params = { 'task;job-subdir': subdir, }
    path = memory.get_path(__name__, key, path_params=path_params)

    async with aiofiles.open(path, mode='r') as fr:
        async for line in fr:
            yield line.strip().split('\t')

#