import logging
import json
import re
import typing

from . import *
from .. import *


__all__ = [
    'text2request',
    'text2response',
    'request2text',
    'response2text',
]

logger = logging.getLogger(__name__)
memory = get_memory()

_SESSION_NAME_PATTERN = r'^[a-zA-Z0-9]{4}[a-z0-9\,\.\-\_\`\~]{,60}$'
_SESSION_NAME_RE = re.compile(_SESSION_NAME_PATTERN)


def validation_job(job):

    if type(job) != str:
        raise TypeError(f'{job}: is not str')

    if not _SESSION_NAME_RE.match(job):
        raise ValueError(f'{job}: is not match pattern={_SESSION_NAME_PATTERN}')

def text2request(arg:str) -> typing.Dict:

    x = json.loads(arg)

    x['worker-key']

    job = x.get('exec-params', {}).get('job')
    if job is not None:
        validation_job(job)

    return x

def text2response(arg:str) -> typing.Dict:

    x = json.loads(arg)

    x['code']

    return x

def request2text(arg:typing.Dict) -> str:

    arg['worker-key']

    x = json_dumps(arg)

    return x

def response2text(arg:typing.Dict) -> str:

    arg['code']

    x = json_dumps(arg)

    return x

#