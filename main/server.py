import logging

def _install_log_level(name:str, level:int):

    # https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility

    logging.addLevelName(level, name.upper())
    setattr(logging, name.upper(), level)

    def _method(self, message, *args, **kwargs):
        if self.isEnabledFor(level):
            self._log(level, message, args, **kwargs)

    setattr(logging.Logger, name.lower(), _method)

_install_log_level('trace', logging.DEBUG - 5)

#_checkLevel = getattr(logging, '_checkLevel')
#_checkLevel(logging.DEBUG - 5)
#_checkLevel('TRACE')

import os
import sys
import platform
import asyncio
import functools
import signal
import importlib
import itertools
import contextlib
import typing
import json
import argparse


logutil = __import__('logutil')
import batchq


logger = logging.getLogger()


def unix_signal_handler(signum, signame):

    logger.info(f'receive signal={signum} signame={signame}')

    batchq.local_end()


async def start_tasks(memory:batchq.Memory):

    names = (
        'regularly',
        'loader',
        'postman',
        'consumer',
        'netlistener',
        'producer',
        'splitter',
        'pipelistener',
    )

    factories = [ importlib.import_module(f'batchq.{x}').new_task_factory() for x in names if x ]
    #factories, factories2 = itertools.tee(factories)

    for factory in factories:
        module_name = factory.__module__

        logger.trace(f'call {module_name}.ainit()')
        await factory.ainit()

        memory.create_event(module_name, 'ready')
        memory.create_event(module_name, 'end')
        memory.create_queue(module_name)

    logger.trace(f'create tasks')
    tasks = [ await factory.create_tasks() for factory in factories ]
    tasks = tuple(itertools.chain.from_iterable(tasks))

    logger.trace(f'vals) {memory.vals}')

    logger.trace(f'asyncio.gather()')
    returns = await asyncio.gather(*tasks)

    logger.trace(f'returns) {returns}')
    #logger.trace(f'memory) {memory}')

    logger.debug('start_tasks() normal end')


class AlreadyRunningError(Exception): ...


@contextlib.asynccontextmanager
async def prepare_files(memory:batchq.Memory):

    other_pid = None
    create_pid_file = False

    try:
        try:
            await memory.helper.load_path_val('batchq', 'last-shutdown', converter=float)

        except ValueError:
            logger.debug('last-shutdown: not defined')

        if platform.system() == 'Linux':

            other_pid = await memory.helper.path_read('batchq', 'pid-save')

            if not batchq.str_is_empty(other_pid):
                if os.path.isdir(f'/proc/{other_pid}'):
                    raise AlreadyRunningError(f'already exists other process pid={other_pid}')

        await memory.helper.path_println('batchq', 'pid-save', os.getpid())

        create_pid_file = True

        yield

    finally:
        if create_pid_file:
            await memory.helper.path_remove('batchq', 'pid-save')

            #await memory.helper.path_println('batchq', 'last-shutdown', batchq.current_timestamp())
            #await memory.helper.path_println('batchq', 'last-memory', memory)


async def aiomain(config:typing.Dict, loglevel):
    try:
        local_end = asyncio.Event()

        loop = asyncio.get_event_loop()
        loop.set_debug(logger.level <= logging.DEBUG)

        for signame in ('SIGINT', 'SIGTERM', 'SIGUSR1', ):

            #signum = signal.SIGUSR1
            signum = getattr(signal, signame)
            fpartial = functools.partial(unix_signal_handler, signum=signum, signame=signame)

            loop.add_signal_handler(signum, fpartial)

        logger.trace(f'library initialize')
        memory = await batchq.init_library(config=config, local_end=local_end)

        '''
        def dump_memory():
            print(f'\n*** (atexit) DUMP MEMORY ***\n{memory}', file=sys.stderr)
        atexit.register(dump_memory)
        '''

        with logutil.raii_app_logger(memory, loglevel):

            logger.trace(f'prepare files')
            async with prepare_files(memory):

                logger.trace(f'start tasks')
                await start_tasks(memory)

    except AlreadyRunningError as e:
        logger.warning(str(e))

    except Exception as e:
        batchq.sys_exit(f'catch exception={type(e)}: {e}')

    else:
        logger.debug('aiomain() normal end')


def setup_root_logger():

    env_loglevel = os.environ.get('BQ_LOGLEVEL', 'INFO')

    try:
        loglevel = getattr(logging, env_loglevel)

    except KeyError:
        loglevel = logging.WARNING

    #
    format = '%(levelname)s: %(pathname)s(%(lineno)d): %(message)s'
    formatter = logging.Formatter(format)

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()

    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel)

    return loglevel


def main(conf_file:str):

    try:
        loglevel = setup_root_logger()

        with open(conf_file, mode='rt') as fr:
            config = json.loads(fr.read())

        asyncio.run(aiomain(config, loglevel))

    except Exception as e:
        logger.exception(f'main')

    else:
        print('main() normal end', file=sys.stderr)


def _xx():


    pass


if __name__ == '__main__':

    print(f'{__file__}: start', file=sys.stderr)
    #_xx()

    if os.environ.get('BQ_CLEAR_BEFORE_MAIN'):

        clear_cmd = 'cls' if platform.system() == 'Windows' else 'clear'
        os.system(clear_cmd)

        print(f'run at path={os.getcwd()}', file=sys.stderr)
        print(os.environ.get('BQ_APP_WORK'))

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', dest='conf_file', required=True, help='Config file to process.')
    cmd_args = parser.parse_args()

    main(cmd_args.conf_file)

    print(f'{__file__}: done.', file=sys.stderr)

'''
/ より左側の引数は 位置専用引数 (positional-only parameters)
* より右側の引数は キーワード専用引数 (keyword-only parameters)
'''

#