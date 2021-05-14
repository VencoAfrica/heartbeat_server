import asyncio
from asyncio.streams import StreamReader, StreamWriter
import json
import getpass
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

async def server_handler(reader: StreamReader, writer: StreamWriter, deps):
    logger = deps['logger']
    data = bytearray()
    peername = writer.get_extra_info('peername')
    while True:
        part = await reader.read(1024)
        data += part
        if not part or part.endswith((b'\n', b'\r', b'\r\n')):
            break

    logger.info("Received %s", data)

    writer.write(b'\x01R1\x021.0.32.7.0.255()\x03x')
    end_chars = [b"\x03", b"\x04"]
    resp = bytearray()
    while True:
        logger.info('read: %s', resp)
        part = reader.read(1)
        resp += part
        if not part or part in end_chars:
            break
    
    logger.info("write response: %s", resp)

    to_push = None
    try:
        parsed = HeartbeartData(data)
        to_push = parsed.get_parsed()
    except:
        logger.exception("badly formed heartbeat. cannot log or respond!")

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
