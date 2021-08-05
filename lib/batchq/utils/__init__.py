import json
import os
import datetime
import collections
import decimal
import uuid
import typing
import asyncio
import aiofiles
import aiofiles.os
import sys
import inspect
import functools
import shutil
import logging
import pprint
import traceback
import deepmerge
import copy
import time


__all__ = [
    'local_datetime',
    'utc_datetime',
    'current_timestamp',
    'here',
    'str_is_empty',
    'sys_exit',
    'expand_placeholder',
    'path_expand_placeholder',
    'json_dumps',
    'func2coro',
    'async_os_makedirs',
    'async_os_path_isdir',
    'async_os_path_exists',
    'async_shutil_rmtree',
    're_create_directory',
    'path_read',
    'path_open',
    'path_write',
    'peername_str2tuple',
    'cmp_hostinfo',
    'dict_deep_merge',
    'on_timer_helper',
    'iscoroutinefunction_or_partial',
]

logger = logging.getLogger(__name__)

_MERGER = deepmerge.Merger([
        (list, ["append"]),
        (dict, ["merge"])
    ],
    ["override"],
    ["override"]
)


def local_datetime(utc=False):
    return datetime.datetime.now()

def utc_datetime():
    return datetime.datetime.now(tz=datetime.timezone.utc)

def current_timestamp():
    return time.time()

def here():

    stack1 = inspect.stack()[1]
    frame0 = stack1[0]
    info = inspect.getframeinfo(frame0)

    return os.path.basename(info.filename), info.lineno, info.function

def str_is_empty(arg) -> bool:

    if arg is None:
        return True

    assert type(arg) == str
    return len(arg) == 0


def sys_exit(arg, **kwargs) -> typing.NoReturn:

    print(f'\n!!!\n!!! stack [arg={arg}]\n!!!', file=sys.stderr)

    print(pprint.pformat(inspect.stack(), indent=4), file=sys.stderr)

    #for i, stack in enumerate(istack):
    #    print(f'{i}) {stack.filename}({stack.lineno})', file=sys.stderr)

    print(f'\n!!!\n!!! tracebakc [arg={arg}]\n!!!', file=sys.stderr)

    traceback.print_exc()

    logger.exception(f'\n!!!\n!!! logger exception arg=[{arg}]\n!!!')

    sys.exit(1)

#_FORMAT_ENV = { f'env;{k}': v for k, v in os.environ.items() }
_FORMAT_APP = {
    f'app;pid': os.getpid(),
}

def expand_placeholder(arg):

    dt = local_datetime()

    path = os.path.expanduser(arg)
    path = os.path.expandvars(path)
    path = dt.strftime(path.format(**_FORMAT_APP))

    return path

def path_expand_placeholder(arg, *, path_params=None):

    dt = local_datetime()

    if path_params is None:
        path_params = {}

    path = os.path.expanduser(arg)
    path = os.path.expandvars(path)
    path = dt.strftime(path.format(**_FORMAT_APP, **path_params))

    return path

def _json_dumps_default(obj):

    if isinstance(obj, collections.Mapping):
        return dict(obj)

    obj_type = type(obj)

    if obj_type == datetime.datetime:
        return obj.isoformat()

    if obj_type == decimal.Decimal:
        return float(obj)

    if obj_type == bytes:
        hex_str = obj.hex()

        if len(hex_str) < 30:
            return hex_str
        else:
            return hex_str[0:30] + '...'

    if obj_type == datetime.timedelta:
        sec = obj.total_seconds()
        return '{} {}'.format(sec//3600, sec%3600//60)

    if obj_type == uuid.UUID:
        return str(obj)

    logger.trace(f'! json_dumps_util force convert by str({obj_type})')

    return str(obj)

def json_dumps(o, **kwargs):

    kwargs.setdefault('default', _json_dumps_default)
    kwargs.setdefault('ensure_ascii', False)

    return json.dumps(o, **kwargs)

def func2coro(outer_arg):

    async def _real(*args, **kwargs):
        loop = asyncio.get_event_loop()
        pfunc = functools.partial(outer_arg, *args, **kwargs)
        return await loop.run_in_executor(None, pfunc)
    return _real

async_os_makedirs = func2coro(os.makedirs)
async_os_path_isdir = func2coro(os.path.isdir)
async_os_path_exists = func2coro(os.path.exists)
async_shutil_rmtree = func2coro(shutil.rmtree)

async def re_create_directory(path):
    await async_shutil_rmtree(path, ignore_errors=True)
    await async_os_makedirs(path, mode=0o700, exist_ok=True)

async def path_read(path, *, defval=None):
    try:
        async with aiofiles.open(path, mode='r') as f:
            value = await f.read()

    except FileNotFoundError:
        logger.warning(f'{path}: file not found')

        return defval

    else:
        return value.strip()

'''
async def path_write(path, value, *, mode='w'):

    path_dir = os.path.dirname(path)
    isdir = await async_os_path_isdir(path_dir)

    if not isdir:
        logger.trace(f'{path_dir}: make dir')
        await async_os_makedirs(path_dir, mode=0o700, exist_ok=True)

    async with aiofiles.open(path, mode=mode) as f:
        wb = await f.write(value)

        return wb, path
'''
async def path_open(path, *, mode='w'):

    path_dir = os.path.dirname(path)
    isdir = await async_os_path_isdir(path_dir)

    if not isdir:
        logger.trace(f'{path_dir}: make dir')
        await async_os_makedirs(path_dir, mode=0o700, exist_ok=True)

    return aiofiles.open(path, mode=mode)


async def path_write(path, value, *, mode='w'):

    #async with aiofiles.open(path, mode=mode) as f:
    #    wb = await f.write(value)
    #    return wb, path

    async with await path_open(path, mode=mode) as f:
        wb = await f.write(value)
        return wb, path



def peername_str2tuple(arg:str):

    if not ':' in arg:
        raise ValueError(f'{arg}: invalid format')

    tmp = arg.strip().split(':')
    return (tmp[0], int(tmp[1]), )

def cmp_hostinfo(l, r):

    lrt = l.get('request-ts')
    rrt = r.get('request-ts')

    if lrt is not None and rrt is not None:

        if lrt < rrt:
            return -7

        if lrt > rrt:
            return 7

    elif lrt is not None:
        return -6

    elif rrt is not None:
        return 6

    lbusyness = l['running'] / l['softlimit']
    rbusyness = r['running'] / r['softlimit']

    if lbusyness < rbusyness:
        return -1

    if lbusyness > rbusyness:
        return 1

    if l['loadavg'][0] < r['loadavg'][0]:
        return -2

    if l['loadavg'][0] > r['loadavg'][0]:
        return 2

    if l['running'] < r['running']:
        return -3

    if l['running'] > r['running']:
        return 3

    if l['softlimit'] > r['softlimit']:
        return -4

    if l['softlimit'] < r['softlimit']:
        return 4

    if l['ping-ts'] > r['ping-ts']:
        return -5

    if l['ping-ts'] < r['ping-ts']:
        return 5

    return 0

def dict_deep_merge(left:typing.Dict, right:typing.Dict):

    left = copy.deepcopy(left)
    right = copy.deepcopy(right)

    #print(f'\n*** LEFT ***\n{left}\n\n*** RIGHT ***\n{right}\n\n', file=sys.stderr)

    ret = _MERGER.merge(left, right)

    #print(f'*** MERGE ***\n{ret}\n\n', file=sys.stderr)

    return ret

async def on_timer_helper(orig_func, num_times, *, devide, remainder):

    if num_times % devide == remainder:
        await orig_func(num_times)

#
# https://stackoverflow.com/questions/52422860/partial-asynchronous-functions-are-not-detected-as-asynchronous
#
def iscoroutinefunction_or_partial(object):

    while isinstance(object, functools.partial):
        object = object.func

    return asyncio.iscoroutinefunction(object)

#