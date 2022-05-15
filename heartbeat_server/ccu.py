from asyncio.streams import StreamReader, StreamWriter
from logging import Logger

from .heartbeat import Heartbeat
from .hes import process_hes_message
from .utils import send


async def ccu_handler(reader: StreamReader,
                      writer: StreamWriter,
                      hes_params: dict,
                      logger: Logger):
    hes_address = hes_params.get('address', '0.0.0.0')
    hes_port = hes_params.get('port', '18902')

    heartbeat = await Heartbeat.read_heartbeat(reader)
    print(f'\n<- Received heartbeat {heartbeat}')
    await heartbeat.send_heartbeat_reply(writer, logger)
    response = await send_to_hes(hes_address,
                                 hes_port,
                                 heartbeat.data)
    if response:
        ccu_payload = await process_hes_message(response)
        ccu_reading = await send_to_ccu(ccu_payload, reader,  writer, logger)
        await send_to_hes(hes_address,
                          hes_port,
                          ccu_reading)
        logger.info(f'CCU Reading {ccu_reading}')
    else:
        raise Exception(f'No response from HES')
    writer.close()


async def send_to_hes(address: str,
                      port: int,
                      msg: bytearray):
    print(f'\n-> Sending to HES {msg}')
    return await send(address, port, msg)


async def send_to_ccu(data, reader, writer, logger=None):
    tries = 0
    response = bytearray()
    while not response and tries < 3:
        if logger is not None:
            logger.info("Trying: %s", tries + 1)
        writer.write(data)
        response = await read_response(reader)
        heartbeat = Heartbeat(response)
        if heartbeat.is_valid():
            await heartbeat.send_heartbeat_reply(writer, logger)
            break
        else:
            response = bytearray()
        tries += 1
    return response


async def read_response(reader: StreamReader):
    return await reader.read(100)
