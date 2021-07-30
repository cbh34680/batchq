from os import curdir
import typing
import asyncio
import importlib
import contextlib
import collections
import logging
import collections.abc
import functools

from . import *
from .utils import *
from .utils import queutil
from .utils import jobutil


__all__ = [
    'new_task_factory',
 ]

logger = logging.getLogger(__name__)
memory = get_memory()


async def on_check_request(request, response):

    exec_params = request['exec-params']

    args = exec_params.get('args')
    if args is not None:
        if type(args) not in (list, tuple, ):
            response['code'] = 400
            response['reason'] = 'ILLEGAL TYPE'
            response['retry-suggestion'] = 'IMPOSSIBLE ANYTIME'
            return False

    kwargs = exec_params.get('kwargs')
    if kwargs is not None:
        if not isinstance(kwargs, collections.abc.Mapping):
            response['code'] = 400
            response['reason'] = 'ILLEGAL TYPE'
            response['retry-suggestion'] = 'IMPOSSIBLE ANYTIME'
            return False

    worker_key = request['worker-key']

    try:
        worker = memory.get_worker(worker_key)

    except KeyError:
        logger.warning(f'{worker_key}: worker not found')

        response['code'] = 404
        response['reason'] = 'NO SUCH WORKER'
        response['retry-suggestion'] = 'POSSIBLE OUTSIDE'
        return False

    if worker['increment-running']:

        softlimit = memory.get_val(__name__, 'softlimit')
        running = memory.get_val(__name__, 'running')

        if running >= softlimit:
            logger.warning(f'queue full: running={running}')

            response['code'] = 503
            response['reason'] = 'QUEUE FULL'
            response['retry-suggestion'] = 'POSSIBLE LATER'
            return False

    return True


async def on_timer(num_times=0):

    peername = memory.get_val(__name__, 'master-host')

    if peername is None:
        logger.warning('master-host not set, no send ping')
        return

    kwargs = memory.get_vals_by_args(__name__, 'softlimit', 'running')
    kwargs.update(memory.get_vals_by_args(__package__, 'hostid', 'hostname', 'loadavg', 'loadavg'))

    kwargs['listen-port'] = memory.get_const('batchq.netlistener', 'listen-port')
    kwargs['ping-ts'] = current_timestamp()

    message = {
        'peername': peername,
        'payload': {
            'worker-key': 'ping',
            'exec-params': {
                'kwargs': kwargs,
            },
        },
    }

    queue = memory.get_queue('batchq.postman')
    await queutil.put(queue, message, where=here())


@contextlib.asynccontextmanager
async def raii_count_running(incr_running):
    try:
        if incr_running:
            running = memory.helper.val_incr(__name__, 'running')
            logger.trace(f'consumer_running++ -> [{running}]')

        yield

    finally:
        if incr_running:
            running = memory.helper.val_decr(__name__, 'running')
            logger.trace(f'consumer_running-- -> [{running}]')


@contextlib.asynccontextmanager
async def raii_set_end_event():
    try:
        active_task = memory.helper.val_incr(__name__, 'active-task')
        logger.trace(f'enter atask is {active_task}')

        yield

    finally:
        active_task = memory.helper.val_decr(__name__, 'active-task')
        logger.trace(f'leave atask is {active_task}')

        assert active_task >= 0

        if active_task == 0:
            memory.get_event(__name__, 'end').set()


async def do_send_report(arg_message, type, exec_params, result):

    peeraddr = arg_message['peername'][0]

    active_hosts = memory.get_val('batchq.producer', 'active-hosts')
    hostinfo = active_hosts.get(peeraddr)
    peerport = memory.get_const('batchq.netlistener', 'listen-port') if hostinfo is None else hostinfo['peername'][1]

    report_message = {
        'peername': (peeraddr, peerport, ),
        'payload': {
            'worker-key': 'report',
            'exec-params': {
                'kwargs': {
                    'worker-key': arg_message['request']['worker-key'],
                    'type': type,
                    'exec-params': exec_params,
                    'result': result,
                },
            },
        },
    }

    logger.debug(f'send report={report_message}')

    queue = memory.get_queue('batchq.postman')
    await queutil.put(queue, report_message, where=here())

    memory.helper.stats_incr(__name__, 'report')


async def _on_message(message:typing.Dict, taskno:int):

    logger.debug(f'task={taskno} receive message={message}')
    assert message is not None

    memory.helper.stats_incr(__name__, 'event')

    request = message['request']
    worker_key = request['worker-key']
    worker = memory.get_worker(worker_key)

    start_dt = utc_datetime()
    start_ts = current_timestamp()

    async with raii_count_running(worker['increment-running']):

        exec_params = dict_deep_merge(worker['exec-params'], request['exec-params'])
        logger.debug(f'exec-params={exec_params}')

        job = exec_params.get('job')
        if job is not None:
            await jobutil.save_data('execute', message['peername'], job, exec_params)

        success = True
        retval = None
        error = None

        try:
            module_name = worker['module']
            handler = worker['handler']

            memory.helper.stats_incr(__name__, 'handler', f'{module_name}::{handler}')
            module = importlib.import_module(module_name)
            coro = getattr(module, handler)

            retval = await coro(taskno, message['peername'], worker_key, worker, exec_params)

        except Exception as e:

            success = False
            error = {
                'reason': type(e),
                'description': str(e),
            }

            logger.error(f'exec_params={exec_params} exception={type(e)}: {e}')

        result = {
            'success': success,
            'retval': retval,
            'error': error,
            'start-dt': str(start_dt),
            'elapsed': current_timestamp() - start_ts,
        }

        logger.debug(f'result={result}')

        job = exec_params.get('job')
        if job is not None:
            await jobutil.save_data('result', message['peername'], job, result)

        if exec_params['send-report']:
            await do_send_report(message, worker['type'], exec_params, result)
            memory.helper.stats_incr(__name__, "send-report")

        memory.helper.stats_incr(__name__, "success" if success else "error")

        return result


async def on_message(message:typing.Dict, taskno:int=0):

    running = memory.get_val(__name__, 'running')
    logger.trace(f'before _on_message(): current running is {running}')

    result = await _on_message(message, taskno)

    running = memory.get_val(__name__, 'running')
    logger.trace(f'after _on_message(): current running is {running}')

    return result


async def on_exec_request(message, response):

    assert response['code'] == 200

    request = message['request']
    worker_key = request['worker-key']

    worker = memory.get_worker(worker_key)

    if worker['when'] == 'immediate':

        result = await on_message(message)
        response['additional'] = result

        return True

    job = request['exec-params'].get('job')
    if job is not None:
        await jobutil.save_data('request', message['peername'], job, request)

    return False


async def _main(taskno:int):

    queue = memory.get_queue(__name__)

    async with raii_set_end_event():
        async for i, message in queutil.get_message_until_eom(queue, where=here()):
            _ = await on_message(message, taskno)


class _MyTaskFactory(TaskFactory):

    def __init__(self):

        set_params = {
            'running': 0,
            'active-task': 0,
        }
        memory.set_val_by_kwargs(__name__, **set_params)

        memory.append_callback_coro('batchq.netlistener', 'on-check-request', on_check_request)
        memory.append_callback_coro('batchq.netlistener', 'on-exec-request', on_exec_request)

        self.num_tasks = memory.get_const(__name__, 'create-task')

    async def ainit(self):

        assert self.num_tasks > 0

        await memory.helper.load_path_val(__name__, 'softlimit', defval=self.num_tasks, converter=int)

        fpartial = functools.partial(on_timer_helper, on_timer, devide=5, remainder=1)
        memory.append_callback_coro('batchq.regularly', 'on-timer', fpartial)

        try:
            await memory.helper.load_path_val(__name__, 'master-host', converter=peername_str2tuple)

        except ValueError:
            logger.warning('master-host: not defined')

        logger.trace(f'append hook: softlimit: unset_val')
        hook = lambda: memory.set_val(__name__, 'softlimit', self.num_tasks)
        memory.append_val_hook(__name__, 'softlimit', 'unset_val', hook=hook)

    async def main(self, taskno:int):
        try:
            await _main(taskno)

        except asyncio.CancelledError:
            sys_exit(f'catch cancel')

        except Exception as e:
            sys_exit(f'catch {type(e)} exception={e}')

        else:
            logger.trace(f'main normal end')

    async def canceller(self):

        await memory.get_event('batchq.netlistener', 'end').wait()

        logger.trace('fired canceller')

        num_tasks = memory.get_const(__name__, 'create-task')
        queue = memory.get_queue(__name__)

        for _ in range(num_tasks):
            await queutil.put_eom(queue, where=here())

    async def create_tasks(self):

        tasks = [
            asyncio.create_task(self.canceller(), name=f'{__name__}:canceller-task'),
        ]

        for i in range(self.num_tasks):

            taskno = i + 1

            name = f'{__name__}:main-task-{taskno}'
            tasks.append(asyncio.create_task(self.main(taskno), name=name))
            logger.trace(f'regist consumer name={name}')

        return tasks


def new_task_factory():
    return _MyTaskFactory()

#