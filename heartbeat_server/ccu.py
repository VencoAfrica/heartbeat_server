import json
import requests

from asyncio.streams import StreamReader, StreamWriter
from logging import Logger
from datetime import datetime

from .heartbeat import read_heartbeat
from .hes import generate_reading_cmd

from .meter_device import get_reading_cmds
from .meter_reading import MeterReading


async def ccu_handler(reader: StreamReader,
                      writer: StreamWriter,
                      hes_server_url: str,
                      logger: Logger):

    heartbeat = await read_heartbeat(reader)
    print(f'\nccu.ccu_handler:17 Received heartbeat {heartbeat}')
    await heartbeat.send_heartbeat_reply(writer)

    meter_no = heartbeat.device_details

    read_cmds = get_reading_cmds()
    readings = {}

    for read_cmd in read_cmds:
        cmd, obis_code = read_cmd
        reading_cmd = await generate_reading_cmd(meter_no, obis_code)
        reading = await get_reading(reading_cmd,
                                    meter_no,
                                    reader, writer,
                                    logger)
        readings[cmd] = [datetime.now(), reading]

    await send_readings(hes_server_url,
                        {
                            'meter_no': meter_no,
                            'readings': readings
                        })
    writer.close()


async def get_reading(reading_cmd,
                      meter_no,
                      reader: StreamReader,
                      writer: StreamWriter,
                      logger=None):
    tries = 0
    meter_reading = None
    response = None
    while not response and tries < 3:
        if logger is not None:
            logger.info("Trying: %s", tries + 1)
        writer.write(reading_cmd)
        response = await reader.read(100)
        meter_reading = MeterReading(response)
        tries += 1
    return meter_reading.get_value_from_response(meter_no)


async def send_readings(hes_server_url, readings: dict):
    """
    Readings format:
    {
        "meter_no": "12345678901",
         "readings": {
              "phase_a_voltage": {
                    "2019-12-11 10:58:37.039404", "<reading>"
                },
                "voltage": {
                    "2019-12-11 10:58:38.039404", "<reading>"
                }
        }
    }
    """
    resp = requests.post(hes_server_url,
                         data=json.dumps(readings, indent=4),
                         headers={'Content-type': 'application/json',
                                  'Accept': 'text/plain'})
    if resp.status_code != 200:
        raise resp.text


