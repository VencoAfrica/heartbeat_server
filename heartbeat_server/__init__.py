import asyncio
import json
import os


from .logger import get_logger
from .heartbeat import Heartbeat
from .redis import Redis
from .hes import hes_handler
from .ccu import ccu_handler

DEFAULT_REQUEST_QUEUE = 'request_queue'

keep_alive = [('keep alive', '0.2.0')]


class DoneWaiting(Exception):
    pass


async def raise_after(timeout):
    await asyncio.sleep(timeout)
    raise asyncio.TimeoutError


def load_config(filename="config.json"):
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f)
    else:
        print("Failed to load configuration files "
              "('%s' Not Found)" % filename)


def wrapper(params: dict):
    async def handler(reader, writer):
        pub, sub = Redis(params).get_redis_pool()
        await ccu_handler(reader, writer, pub, params)
        await hes_handler(reader, writer, sub)
    return handler


async def heartbeat_server(params=None):
    if params.get('log'):
        params['logger'] \
            = get_logger(params.get('log'))

    params['counts'] = {}
    params['counts_lock'] = asyncio.Lock()
    params['redis_lock'] = asyncio.Lock()

    host = params.get('tcp', {}).get('host', '0.0.0.0')
    port = params.get('tcp', {}).get('port', '18901')

    server = await asyncio.start_server(wrapper(params), host, port)
    addr = server.sockets[0].getsockname()

    if params['logger']:
        params['logger'].info('Serving on %s', addr)

    try:
        async with server:
            await server.serve_forever()
    except:
        if params['logger']:
            params['logger'].exception("Server loop exception")
        raise
