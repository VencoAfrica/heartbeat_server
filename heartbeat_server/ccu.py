import json

from asyncio.streams import StreamReader, StreamWriter
from .redis import Redis
from .heartbeat import Heartbeat


async def ccu_handler(reader: StreamReader, writer: StreamWriter,
                      pub_redis: Redis,
                      params: dict):
    logger = params['logger']
    heartbeat = await Heartbeat.read_heartbeat(reader, logger)
    await Heartbeat.send_heartbeat_reply(heartbeat, writer, logger)
    device_details = heartbeat['device_details']
    await pub_redis.publish('REQUEST: {}|{}'.format(device_details,
                                                    json.dumps(heartbeat.parse())))
    writer.close()
