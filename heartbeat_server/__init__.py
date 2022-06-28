import asyncio
import json
import os

from logging import Logger
from .logger import get_logger
from .heartbeat import Heartbeat
from .hes import generate_reading_cmd
from .ccu import ccu_handler
from .bulk_requests import run_bulk_requests_handler


def write_request_server(params=None):
    if params.get('log'):
        logger = get_logger(params.get('log'))

    bwr_params = params.get('bulk_write_requests')
    redis_params = params.get('redis')

    run_bulk_requests_handler(logger,
                              bwr_params,
                              redis_params)


async def heartbeat_server(params=None):
    logger = None
    if params.get('log'):
        logger = get_logger(params.get('log'))

    await run_server(params.get('ccu', {}),
                     params.get('redis', {}),
                     params.get('hes_server_url', 'localhost/receive_readings'),
                     ccu, logger)


async def run_server(ccu_params: dict,
                     hes_params: dict,
                     redis_params: dict,
                     callback,
                     logger: Logger):
    host = ccu_params.get('host', '0.0.0.0')
    port = ccu_params.get('port', '18901')
    name = ccu_params.get('name', '')

    server = await asyncio.start_server(
        callback(hes_params, redis_params, logger),
        host, port)
    addr = server.sockets[0].getsockname()

    if logger:
        logger.info(f'Waiting for {name} on {addr}')

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
        logger: Logger):
    async def handler(reader, writer):
        await ccu_handler(reader, writer,
                          hes_server_url,
                          redis_params,
                          logger)

    return handler
