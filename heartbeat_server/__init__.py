import asyncio
from asyncio.streams import StreamReader, StreamWriter
import json
import getpass
from logging import Logger
import os
import sys

import aioredis

from iec62056_21.messages import CommandMessage

from heartbeat_server.logger import get_logger
from heartbeat_server.parser import HeartbeartData, prep_data


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

def prepare_config_schedule(config):
    schedule = config.get('schedule', {})
    by_meters = {}
    for obis_code, meters in schedule.items():
        for meter in meters:
            if meter not in by_meters:
                by_meters[meter] = []
            by_meters[meter].append(obis_code)
    config['schedule'] = by_meters
    return config

async def get_scheduled_values(reader, writer, schedule, logger=None):
    ''' return values dict with meter as key '''

    logger.info("Getting schedule data: %s", schedule)
    out = {k: [] for k in schedule}
    for meter, obis_codes in schedule.items():
        for obis_code in obis_codes:
            msg = CommandMessage.for_single_read(obis_code)
            data = prep_data(meter, '01', '82', '33333333', msg.to_bytes())
            response = await send_data(data, reader, writer, logger)
            if response:
                response += b"|" + bytes(obis_code, encoding='utf-8')
                out[meter].append(response)
    return out

async def send_data(data, reader, writer, logger=None):
    tries = 0
    response = bytearray()
    while not response and tries < 3:
        if logger is not None:
            logger.info("Trying: %s", tries+1)
        await asyncio.sleep(1)
        writer.write(data)

        await asyncio.sleep(1)
        response = await read_response(reader, logger=logger, timeout=3.0)
        tries += 1
    return response

async def server_handler(reader: StreamReader, writer: StreamWriter, deps):
    logger = deps['logger']
    peername = writer.get_extra_info('peername')
    data = await HeartbeartData.read_heartbeat(reader, logger)

    logger.info("Received %s", data.hex())
    to_push = None
    parsed = None
    try:
        parsed = HeartbeartData(data)
        to_push = parsed.get_parsed()
    except:
        logger.exception("badly formed heartbeat. cannot log or respond!")

    if parsed is not None:
        reply = parsed.get_reply()
        logger.info("Sending Server Reply: %s", reply.hex())
        await asyncio.sleep(1)
        writer.write(reply)

    try:
        scheduled_data = await get_scheduled_values(
            reader, writer, deps['config']['schedule'], logger
        )
    except KeyError:
        scheduled_data = {}

    if to_push:
        to_push['peername'] = peername
        device_details = to_push['device_details']
        to_push = json.dumps(to_push, indent=2)
        # push heartbeat
        await push_to_queue(device_details, to_push, deps)
        # push scheduled data
        for meter, responses in scheduled_data.items():
            for response in responses:
                await push_to_queue(meter, response, deps)
        logger.info("Parsed: %s", to_push)

    writer.close()




'''
config.json sample:
{
	"tcp":{
		"port": 18901,
	},
	"redis_server_url": "redis://test:bb5qFU9xFMPCWpEJoKOe60zSN1e6LOkT@redis-10661.c259.us-central1-2.gce.cloud.redislabs.com:10661",
	"schedule": {
		"0.0.0.9.1.255": ["179000222382"]
	}
}

'''

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
    config = prepare_config_schedule(config)
 
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
