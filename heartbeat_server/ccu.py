import json
import asyncio

from logging import Logger
from .utils import send
from .hes import process_hes_message

from asyncio.streams import StreamReader, StreamWriter
from .heartbeat import Heartbeat


async def ccu_handler(reader: StreamReader,
                      writer: StreamWriter,
                      hes_params: dict,
                      logger: Logger):
    hes_address = hes_params.get('address', '0.0.0.0')
    hes_port = hes_params.get('port', '18902')

    heartbeat = await Heartbeat.read_heartbeat(reader, logger)
    await heartbeat.send_heartbeat_reply(writer, logger)
    res = await send_to_hes(hes_address,
                            hes_port,
                            json.dumps(heartbeat.parse()))
    ccu_payload = await process_hes_message(res)
    ccu_reading = await send_to_ccu(ccu_payload, reader,  writer, logger)
    ccu_reading_response = await send_to_hes(hes_address,
                                             hes_port,
                                             ccu_reading.decode())
    logger.info(f'CCU Reading {ccu_reading_response}')
    writer.close()


async def send_to_hes(address: str,
                      port: int,
                      msg: str):
    await send(address, port, msg)


async def send_to_ccu(data, reader, writer, logger=None):
    tries = 0
    response = bytearray()
    while not response and tries < 3:
        if logger is not None:
            logger.info("Trying: %s", tries + 1)
        await asyncio.sleep(1)
        writer.write(data)

        await asyncio.sleep(1)
        response = await read_response(reader, logger=logger, timeout=3.0)
        heartbeat = Heartbeat(response)
        if heartbeat.is_valid():
            await heartbeat.send_heartbeat_reply(writer, logger)
            response = bytearray()
        tries += 1
    return response


async def read_response(reader: StreamReader, end_chars=None,
                        buf_size=1, timeout=10.0, logger: Logger = None):
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
