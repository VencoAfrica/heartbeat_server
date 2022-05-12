import json

from asyncio.streams import StreamReader, StreamWriter
from .hs_redis import Redis
from .heartbeat import Heartbeat


async def ccu_handler(reader: StreamReader, writer: StreamWriter,
                      redis: Redis,
                      params: dict):
    logger = params['logger']
    heartbeat = await Heartbeat.read_heartbeat(reader, logger)
    await heartbeat.send_heartbeat_reply(writer, logger)
    await redis.publish(message=json.dumps(heartbeat.parse()))
    writer.close()
