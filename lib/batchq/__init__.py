import os
import typing
import asyncio
import json
import aiofiles
import aiofiles.os
import sys
import abc
import pprint
import logging
import uuid
import socket

from .utils import *


__all__ = [
    'TaskFactory',
    'NamedQueue',
    'Memory',
    'init_library',
    'get_memory',
    'PropObject',
]

logger = logging.getLogger(__name__)

_EMPTY_WORKER = {
    'when': 'later',
    'type': 'subproc',
    'module': 'batchq.handlers.simple',
    'function': 'handle_any',
    'increment-running': True,
    'exec-params': {
        'args': [],
        'kwargs': {},
        'send-report': True,
    },
}


class TaskFactory(metaclass=abc.ABCMeta):
    async def ainit(self): ...

    @abc.abstractmethod
    async def create_tasks(self): ...


class NamedQueue(asyncio.Queue):

    def __init__(self, name, *args, **kwargs):
        self.name = name
        super().__init__(*args, **kwargs)

    def __str__(self):
        return f'{self.name}:{super().__str__()}'


class Memory():
    def __init__(self, config:typing.Dict, world_end:asyncio.Event):

        self.config = config

        self.events = {
            __package__: {
                'world-end': world_end,
            },
        }

        self.queues = {}
        self.vals = {
            __package__: {
                'hostid': str(uuid.uuid4()),
                'hostname': socket.gethostname(),
                'boot-dt': local_datetime(),
                'boot-ts': current_timestamp(),
            },
        }
        self.valhooks = {}
        self.callback_coros = {}

        class Helper():

            def __init__(self, owner):
                self.owner:Memory = owner

            async def path_read(self, domain, key, *, defval=None, path_params=None):

                path = self.owner.get_path(domain, key, path_params=path_params)
                return await path_read(path, defval=defval)

            async def path_write(self, domain, key, value, *, mode='w', path_params=None):

                path = self.owner.get_path(domain, key, path_params=path_params)
                return await path_write(path, value, mode=mode)

            async def path_println(self, domain, key, value, **kwargs):

                return await self.path_write(domain, key, f'{value}\n', **kwargs)

            async def path_append_tsvln(self, domain, key, *args, mode='a', path_params=None):

                outarr = [ utc_datetime().isoformat(), ] + [ str(x).strip() for x in args ]
                outline = '\t'.join(outarr) + '\n'

                return await self.path_write(domain, key, outline, mode=mode, path_params=path_params)

            async def path_touch(self, domain, key, *, path_params=None):

                return await self.path_write(domain, key, '', path_params=path_params)

            def path_exist(self, domain, key, *, path_params=None):

                path = self.owner.get_path(domain, key, path_params=path_params)
                return os.path.exists(path)

            async def path_stat(self, domain, key, *, path_params=None):

                path = self.owner.get_path(domain, key, path_params=path_params)
                return await aiofiles.os.stat(path)

            async def path_remove(self, domain, key, *, path_params=None):

                path = self.owner.get_path(domain, key, path_params=path_params)

                exist = await async_os_path_exists(path)
                if exist:
                    await aiofiles.os.remove(path)

            async def path_rename(self, domain, key, *, orig, path_params=None):

                path = self.owner.get_path(domain, key, path_params=path_params)

                path_dir = os.path.dirname(path)
                isdir = await async_os_path_isdir(path_dir)

                if not isdir:
                    logger.trace(f'{path_dir}: make dir')
                    await async_os_makedirs(path_dir, mode=0o700, exist_ok=True)

                return await aiofiles.os.rename(orig, path)

            async def load_path_val(self, domain, key, *, defval=None, converter=None, path_params=None):

                value = await self.path_read(domain, key, defval=defval, path_params=path_params)

                if value is None:
                    raise ValueError('value is None')

                if converter is not None:
                    try:
                        value = converter(value)

                    except ValueError as e:
                        logger.error(f'catch {type(e)} exception={e}, set default({defval})')
                        value = defval

                if value is not None:
                    self.owner.set_val(domain, key, value)

                return value

            def val_incr(self, domain, key, *, initval=None):

                value = self.owner.get_val(domain, key, defval=initval)
                value += 1
                self.owner.set_val(domain, key, value)

                return value

            def val_decr(self, domain, key):

                value = self.owner.get_val(domain, key)
                if value == 0:
                    sys_exit(f'value eq 0')

                value -= 1
                self.owner.set_val(domain, key, value)

                return value

            def stats_incr(self, domain, key, subkey=None):

                key = str(key)

                stats = self.owner.get_val(domain, 'stats')

                if stats is None:
                    stats = self.owner.set_val(domain, 'stats', {})

                if subkey is None:
                    stats[key] = stats.setdefault(key, 0) + 1

                else:
                    keyed_stats = stats.setdefault(key, {}).setdefault('*', {})
                    keyed_stats[subkey] = keyed_stats.setdefault(subkey, 0) + 1

        self.helper = Helper(self)

    def raw(self):

        ret = {
            'events': self.events,
            'queues': self.queues,
            'valhooks': self.valhooks,
            'callback_coros': self.callback_coros,
            'vals': self.vals,
        }

        return ret

    def __str__(self):

        return '\n\n'.join([ f'[{k}]\n{pprint.pformat(v, indent=4, width=100)}' for k, v in self.raw().items() ])

    def get_const(self, domain, key):

        return self.config['const'][domain][key]

    def get_path(self, domain, key, *, path_params=None):

        return os.path.realpath(path_expand_placeholder(self.config['path'][domain][key], path_params=path_params))

    def create_event(self, domain, key) -> asyncio.Event:

        self.events.setdefault(domain, {})
        event = asyncio.Event()
        self.events[domain][key] = event
        return event

    def get_event(self, domain, key) -> asyncio.Event:

        return self.events[domain][key]

    def create_queue(self, domain, key='default') -> asyncio.Queue:

        self.queues.setdefault(domain, {})
        #queue = asyncio.Queue()
        queue = NamedQueue(f'{domain}:{key}')
        self.queues[domain][key] = queue
        return queue

    def get_queue(self, domain, key='default') -> asyncio.Queue:

        return self.queues[domain][key]

    def set_val(self, domain, key, value, *, private=False):

        if value is None:
            logger.warning('set_val call by value=None')
            raise ValueError

        if private:
            key += f'#{id(value)}'

        self.vals.setdefault(domain, {})
        self.vals[domain][key] = value

        self.call_val_hooks(domain, key, sys._getframe().f_code.co_name)

        return value

    def set_val_by_kwargs(self, domain, *, private=False, **kwargs):

        return [ self.set_val(domain, k, v, private=private) for k, v in kwargs.items() ]

    def unset_val(self, domain, key):
        try:
            del self.vals[domain][key]

        except KeyError:
            logger.warning(f'{domain}.{key}: not found, ignore')

        else:
            self.call_val_hooks(domain, key, sys._getframe().f_code.co_name)

    def get_val(self, domain, key, *, defval=None):
        try:
            value = self.vals[domain][key]
            assert value is not None

        except KeyError:
            logger.warning(f'{domain}.{key}: not found')
            return defval

        else:
            return value

    def get_vals_by_args(self, domain, *args, defval=None):

        return { k: self.get_val(domain, k, defval=defval) for k in args }

    def append_val_hook(self, domain, key, opname, hook):

        hooks = self.valhooks.setdefault(domain, {}).setdefault(key, {}).setdefault(opname, [])

        if hook in hooks:
            logger.warning(f'already installed {hook}, ignore')

        else:
            hooks.append(hook)

    def call_val_hooks(self, domain, key, opname, *args, **kwargs):
        try:
            hooks = self.valhooks[domain][key][opname]

        except KeyError:
            logger.trace(f'{domain}.{key}:{opname}: vals-hook not found')
            return []

        else:
            hooks = [ hook(*args, **kwargs) for hook in hooks ]
            return hooks

    def append_callback_coro(self, domain:str, key:str, coro:typing.Coroutine):

        coros = self.callback_coros.setdefault(domain, {}).setdefault(key, [])

        if coro in coros:
            logger.warning(f'already installed coro={coro}, ignore')

        else:
            coros.append(coro)


    async def call_callback_coros_until_if(self, domain:str, key:str, *args, predicate, **kwargs):
        try:
            coros = self.callback_coros[domain][key]

        except KeyError:
            logger.trace(f'{domain}: {key}: callback-coro not found')

        else:
            for coro in coros:

                ret = await coro(*args, **kwargs)

                if predicate(ret):
                    return True

        return False

    async def call_callback_coros(self, domain:str, key:str, *args, **kwargs):
        try:
            coros = self.callback_coros[domain][key]

        except KeyError:
            logger.trace(f'{domain}: {key}: callback-coro not found')
            return []

        else:
            coros = [ await coro(*args, **kwargs) for coro in coros ]
            return coros

    def get_worker(self, key) -> typing.Dict:

        worker = self.config['worker'][key]
        worker = dict_deep_merge(_EMPTY_WORKER, worker)

        return worker

    async def reload_workers(self):

        try:
            worker_config = await self.helper.path_read(__package__, 'worker-config')

            if worker_config is not None:
                worker = json.loads(worker_config)
                self.config['worker'] = worker

        except json.JSONDecodeError:
            logger.warning('worker-config read error')

    def clear_workers(self):

        self.config['worker'] = {}

    async def ainit(self):

        await self.reload_workers()


async def init_library(config:typing.Dict, world_end:asyncio.Event):

    global _memory

    _memory = Memory(config, world_end)
    await _memory.ainit()

    return _memory

def get_memory():

    return _memory

class PropObject(object):

    def __init__(self, **kwargs):

        self.__dict__.update(**kwargs)

    def __repr__(self):

        return str(self.__dict__)

#