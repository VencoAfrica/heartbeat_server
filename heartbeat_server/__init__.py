import asyncio
from asyncio.streams import StreamReader, StreamWriter
import json
import getpass
from logging import Logger
import os
import sys

import aioredis

from heartbeat_server.logger import get_logger
from heartbeat_server.parser import HeartbeartData


def load_config(filename="config.json", deps=None):
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f)
    elif deps is not None and 'logger' in deps:
        deps['logger'].error(
            "Failed to load configuration files ('%s' Not Found)" % filename)

async def redis_pool(url, deps=None):
    # Redis client bound to pool of connections (auto-reconnecting).
    deps = {} if deps is None else deps
    if 'redis' in deps:
        return deps['redis']

    conn = await aioredis.create_redis_pool(url)
    deps['redis'] = conn
    return conn

async def get_redis(deps):
    url = deps['config']['redis_server_url']
    return await redis_pool(url, deps)

async def push_to_queue(queue_id, message, deps):
    logger = deps.get('logger')
    redis = None
    try:
        redis = await get_redis(deps)
    except:
        if logger:
            logger.exception("Error getting redis connection")

    if redis is not None:
        try:
            await redis.lpush(queue_id, message)
        except:
            if logger:
                logger.exception("Error pushing message to queue")

async def read_response(reader: StreamReader, end_chars=None,
                        buf_size=1, timeout=10.0, logger: Logger =None):
    data = bytearray()
    end_chars = end_chars if end_chars is not None else []
    try:
        while True:
            part = await asyncio.wait_for(reader.read(buf_size), timeout=timeout)
            data += part
            if not part or part in end_chars:
                break
    except asyncio.TimeoutError:
        if logger is not None:
            logger.exception("timeout reading response after %ss", timeout)
    return data

async def server_handler(reader: StreamReader, writer: StreamWriter, deps):
    logger = deps['logger']
    peername = writer.get_extra_info('peername')
    data = await HeartbeartData.read_heartbeat(reader, logger)

    logger.info("Received %s", data)
    to_push = None
    parsed = None
    try:
        parsed = HeartbeartData(data)
        to_push = parsed.get_parsed()
    except:
        logger.exception("badly formed heartbeat. cannot log or respond!")

    if parsed is not None:
        logger.info("Sending Server Reply: %s", parsed.get_reply())
        await asyncio.sleep(1)
        writer.write(parsed.get_reply())

    tries = 0
    response = bytearray()
    to_send = bytearray([0x68, 0x82, 0x23, 0x22, 0x00, 0x90, 0x17, 0x68, 0x01, 0x17, 0x77, 0x77, 0x33, 0x52, 0x63, 0xE5, 0x34, 0x85, 0x64, 0x35, 0x63, 0x61, 0x65, 0x61, 0x63, 0x61, 0x65, 0x68, 0x68, 0x5B, 0x5C, 0x36, 0x80, 0xE5, 0x16])
    while not response and tries < 3:
        logger.info("Trying: %s", tries+1)
        await asyncio.sleep(1)
        writer.write(to_send)

        await asyncio.sleep(1)
        response = await read_response(reader, logger=logger)
        tries += 1

    logger.info("write response: %s", response)

    if to_push:
        to_push['peername'] = peername
        device_details = to_push['device_details']
        to_push = json.dumps(to_push, indent=2)
        await push_to_queue(device_details, to_push, deps)
        logger.info("Parsed: %s", to_push)

    writer.close()

def wrapper(deps):
    async def handler(reader, writer):
        await server_handler(reader, writer, deps)
    return handler

async def main(deps=None):
    if deps is None:
        deps = {}

    sys.stderr.write("\ncurrent user: %s\n" % getpass.getuser())
    logger = deps.get('logger')
    if logger is None:
        logger = get_logger()

    config = deps.get('config')
    if config is None:
        config = load_config()
 
    deps['logger'] = logger
    deps['config'] = config

    host = config.get('tcp', {}).get('host', '0.0.0.0')
    port = config.get('tcp', {}).get('port', '18901')

    server = await asyncio.start_server(wrapper(deps), host, port)
    addr = server.sockets[0].getsockname()
    logger.info('Serving on %s', addr)

    try:
        async with server:
            await server.serve_forever()
    except:
        logger.exception("Server loop exception")
        raise
