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


async def read_data(reader: StreamReader, ouput: bytearray):
    end_chars = [b"\x03", b"\x04"]
    while True:
        part = await reader.read(1)
        ouput.extend(part)
        if not part or part in end_chars:
            break
 
async def send_data(reader: StreamReader, writer: StreamWriter,
                    to_send: bytes, logger: Logger):
    await asyncio.sleep(1)
    writer.write(to_send)
    # await writser.drain()

    output = bytearray()
    try:
        await asyncio.sleep(1)
        await asyncio.wait_for(read_data(reader, output), timeout=10.0)
    except asyncio.TimeoutError:
        logger.exception('timeout!')
    return output

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

    tries = 0
    response = bytearray()
    end_chars = [b"\x03", b"\x04"]
    # to_send = b'\x01\x52\x31\x02\x30\x2E\x32\x2E\x30\x2E\x32\x35\x35\x28\x29\x03\x4D'
    to_send = b'\x01\x52\x31\x02\x30\x2e\x30\x2e\x30\x2e\x39\x2e\x31\x2e\x32\x35\x35\x28\x29\x03\x47'
    while not response and tries < 3:
        # response = await send_data(reader, writer, to_send, logger)
        logger.info("Trying: %s", tries)
        await asyncio.sleep(1)
        writer.write(to_send)
        # await writer.drain()

        try:
            await asyncio.sleep(1)
            # await asyncio.wait_for(read_data(reader, output), timeout=10.0)
            while True:
                part = await asyncio.wait_for(reader.read(1), timeout=10.0)
                response.extend(part)
                if not part or part in end_chars:
                    break
        except (asyncio.TimeoutError, BrokenPipeError):
            logger.exception('Timeout or pipe broken!')

        tries += 1

    logger.info("write response: %s", response)

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
