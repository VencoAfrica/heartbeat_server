import asyncio
import json
import os

from logging import Logger
from .logger import get_logger
from .heartbeat import Heartbeat
from .hes import generate_reading_cmd
from .ccu import ccu_handler


async def heartbeat_server(params=None):
    logger = None
    if params.get('log'):
        logger = get_logger(params.get('log'))

    await run_server(params.get('ccu', {}),
                     params.get('hes').get('server_url',
                                           'localhost/receive_readings'),
                     params.get('redis', {}),
                     params.get('hes').get('auth_token'),
                     params.get('auth_token'),
                     ccu, logger)


async def run_server(ccu_params: dict,
                     hes_params: dict,
                     redis_params: dict,
                     hes_auth_token: str,
                     auth_token: str,
                     callback,
                     logger: Logger):
    host = ccu_params.get('host', '0.0.0.0')
    port = ccu_params.get('port', '18901')
    name = ccu_params.get('name', '')

    server = await asyncio.start_server(
        callback(hes_params, redis_params,
                 hes_auth_token, auth_token, logger),
        host, port)
    addr = server.sockets[0].getsockname()

    if logger:
        logger.info(f'Starting {name} on {addr}')

    try:
        async with server:
            await server.serve_forever()
    except Exception:
        if logger:
            logger.exception("Server loop exception")
        raise


def load_config(filename="config.json"):
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f)
    else:
        print("Failed to load configuration files "
              "('%s' Not Found)" % filename)


def ccu(hes_server_url: str,
        redis_params: dict,
        hes_auth_token: str,
        auth_token: str,
        logger: Logger):
    async def handler(reader, writer):
        await ccu_handler(reader, writer,
                          hes_server_url,
                          redis_params,
                          auth_token,
                          hes_auth_token,
                          logger)

    return handler
