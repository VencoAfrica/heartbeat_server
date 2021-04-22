import asyncio
import json
import os

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
    redis = await get_redis(deps)
    await redis.lpush(queue_id, message)


async def _parse_heartbeat(in_data, deps):
    logger = deps['logger']
    try:
        parsed = HeartbeartData(in_data)

        msg = (
            "version_number: %s\n"
            "source_address: %s\n"
            "target_address: %s\n"
            "frame_length: %s\n"
            "fixed_format: %s\n"
        ) % (
            bytearray(parsed.version_number).hex(),
            bytearray(parsed.target_address).hex(),
            bytearray(parsed.source_address).hex(),
            bytearray(parsed.fixed_format_length).hex(),
            bytearray(parsed.fixed_format).hex()
        )

        out_data = parsed.output_data
        msg += 'heartbeat response: %s' % bytearray(out_data).hex()
        logger.info(msg)

        # meter = parsed.version_number.decode()
        await push_to_queue('meter', 'json data', deps)
    except IndexError:
        logger.exception("badly formed heartbeat. cannot log or respond!")
        out_data = b'\xFF'
        
    return out_data

async def server_handler(reader, writer, deps):
    logger = deps['logger']
    data = bytearray()
    while True:
        part = await reader.read(100)
        data += part
        if not part or part.endswith((b'\n', b'\r', b'\r\n')):
            break

    logger.info("Received %s", data)

    out_data = await _parse_heartbeat(data, deps)

    logger.info("Parsed %s", out_data)

    writer.close()

def wrapper(deps):
    async def handler(reader, writer):
        await server_handler(reader, writer, deps)
    return handler

async def main(deps=None):
    if deps is None:
        deps = {}

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
