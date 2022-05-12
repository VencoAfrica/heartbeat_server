import asyncio
import json
import os


from .logger import get_logger
from .heartbeat import Heartbeat
from .hs_redis import Redis
from .hes import hes_handler
from .ccu import ccu_handler


async def heartbeat_server(params=None):
    if params.get('log'):
        params['logger'] \
            = get_logger(params.get('log'))

    host = params.get('tcp', {}).get('host', '0.0.0.0')
    port = params.get('tcp', {}).get('port', '18901')

    redis = Redis(params['redis'])
    await redis.get_redis_pool()

    server = await asyncio.start_server(wrapper(params, redis), host, port)
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


def load_config(filename="config.json"):
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f)
    else:
        print("Failed to load configuration files "
              "('%s' Not Found)" % filename)


def wrapper(params: dict, redis: Redis):
    async def handler(reader, writer):
        await ccu_handler(reader, writer, redis, params)
    return handler
