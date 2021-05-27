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


DEFAULT_PASSWORD_LEVEL =  "01"
DEFAULT_PASSWORD = "33333333"
DEFAULT_RANDOM_NUMBER = "82"
DEFAULT_REQUEST_QUEUE = 'request_queue'

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

async def get_scheduled_values(reader, writer, schedule, config, logger=None):
    ''' return values dict with meter as key '''

    logger.info("Getting schedule data: %s", schedule)
    out = {k: [] for k in schedule}
    password_level = config.get("password_level", DEFAULT_PASSWORD_LEVEL)
    password = config.get("password", DEFAULT_PASSWORD)
    random_number= config.get("random_number", DEFAULT_RANDOM_NUMBER)

    for meter, obis_codes in schedule.items():
        for obis_code in obis_codes:
            msg = CommandMessage.for_single_read(obis_code)
            data = prep_data(
                meter_no=meter,
                pass_lvl=password_level,
                random_no=random_number,
                passw=password,
                data=msg.to_bytes()
            )
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

async def serve_requests_from_frappe(
    reader: StreamReader, writer: StreamWriter, deps, timeout=60*5
):
    config = deps['config']
    logger = deps['logger']
    request_queue = config.get('request_queue', DEFAULT_REQUEST_QUEUE)
    password_level = config.get("password_level", DEFAULT_PASSWORD_LEVEL)
    password = config.get("password", DEFAULT_PASSWORD)
    random_number= config.get("random_number", DEFAULT_RANDOM_NUMBER)
    logger.info("Waiting for frappe requests...")

    async def frappe_server():
        redis = await get_redis(deps)
        while True:
            req = await redis.blpop(request_queue, timeout=timeout-60)
            splitted = req[1].split(b'|', 2) if req else []
            if len(splitted) != 3:
                continue
            key, meter, data = splitted
            to_send = prep_data(
                meter_no=meter.decode(),
                pass_lvl=password_level,
                random_no=random_number,
                passw=password,
                data=data
            )
            logger.info(
                "...handling frappe request %s \n(Original: %s)",
                to_send.hex(), req
            )
            response = await send_data(to_send, reader, writer, logger) or b''
            logger.info(
                "...frappe response for key %s: %s", key, response.hex()
            )
            if response:
                await push_to_queue(key.decode(), response, deps)

    try:
        part = await asyncio.wait_for(frappe_server(), timeout=timeout)
    except (asyncio.TimeoutError, BrokenPipeError):
        logger.exception("error while serving request from frappe")

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

    if to_push:
        to_push['peername'] = peername
        device_details = to_push['device_details']
        to_push = json.dumps(to_push, indent=2)
        logger.info("Parsed: %s", to_push)
        # push heartbeat
        await push_to_queue(device_details, to_push, deps)
        # serve requests from frappe
        await serve_requests_from_frappe(reader, writer, deps, timeout=5*60)

    writer.close()




'''
config.json sample:
{
	"tcp":{
		"port": 18901,
	},
	"redis_server_url": "redis://usr:pwd@redis-url:port",
	"schedule": {
		"0.0.0.0.0.255": ["179000000000"]
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
