import asyncio
import itertools
import logging

from .. import *


__all__ = [
    'NetworkError',
    'TERMINATER',
    'readlines',
    'writeline',
    'readlines_after_write',
]

pkg_logger = logging.getLogger(__package__)
logger = logging.getLogger(__name__)
memory = get_memory()


class NetworkError(Exception): ...
TERMINATER = ''

async def readlines(reader:asyncio.StreamReader, *, timeout=None, peername=None, where=None):

    try:
        for i in itertools.count():

            line = await asyncio.wait_for(reader.readline(), timeout)
            line = line.decode('utf-8')

            logger.trace(f'READ: {where}: {peername}: {timeout}: [[{line}]]')

            line = line.strip('\r\n')

            if line == '':
                logger.trace(f'# READ: {where}: {peername}: TERMINATE')
                break

            yield i, line

    except asyncio.TimeoutError:
        pkg_logger.warning(f'timeout peername={peername}')
        logger.warning(f'# READ: {where}: {peername}: TIMEOUT')

    except (ConnectionRefusedError, FileNotFoundError, ) as e:
        logger.warning(f'# READ: {where}: {peername}')
        raise NetworkError(f'from: {type(e)}: {e}') from e

    except Exception as e:
        pkg_logger.error(f'peername={peername} exception={type(e)}: {e}')
        logger.error(f'# READ: {where}: {peername}: {type(e)}: {e}')
        raise


async def writeline(writer:asyncio.StreamWriter, arg=None, *, timeout=None, peername=None, where=None):

    arg_stripped = '' if arg is None else arg.strip('\r\n')

    try:
        writestr = arg_stripped + '\r\n'

        writer.write(writestr.encode('utf-8'))
        await asyncio.wait_for(writer.drain(), timeout)

        logger.trace(f'WRITE: {where}: {timeout}: {peername}: [[{writestr}]]')

        if arg_stripped == '':
            logger.trace(f'# WRITE: {where}: {peername}: TERMINATE')

    except (ConnectionRefusedError, FileNotFoundError, ) as e:
        logger.warning(f'# WRITE: {where}: {peername}: {type(e)}: {e}')
        raise NetworkError(f'from: {type(e)}: {e}') from e

    except Exception as e:
        pkg_logger.error(f'peername={peername} exception={type(e)}: {e}')
        logger.error(f'# WRITE: {where}: {peername}: {type(e)}: {e}')
        raise


async def readlines_after_writelines(peername:tuple, arg, *, timeout=None, where=None):

    writer = None

    try:
        logger.trace(f'# OPEN: {where}: {peername}: {timeout}')

        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(*peername), timeout)

        except (ConnectionRefusedError, FileNotFoundError, ) as e:
            logger.warning(f'# OPEN: {where}: {peername}: EXCEPTION: {type(e)}: {e}')
            raise NetworkError(f'from: {type(e)}: {e}') from e

        else:
            if type(arg) == str:
                text = arg.strip()

            else:
                text = '\r\n'.join(x.strip() for x in arg)

            await writeline(writer, text,       timeout=timeout, peername=peername, where=where)
            await writeline(writer, TERMINATER, timeout=timeout, peername=peername, where=where)

            async for i, line in readlines(reader, timeout=timeout, peername=peername, where=where):

                yield i, line

    finally:
        if writer is not None:
            logger.trace(f'# CLOSE: {where}: {peername}')

            writer.close()
            await writer.wait_closed()

#