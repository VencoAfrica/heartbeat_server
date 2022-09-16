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
                      redis_params: dict,
                      auth_token: str,
                      logger: Logger):
    heartbeat = await read_heartbeat(reader, logger)
    if logger:
        logger.info(f'\nccu.ccu_handler(): Received heartbeat {heartbeat}')

    reply_resp = await heartbeat.send_heartbeat_reply(logger, reader, writer)
    logger.info('Heartbeat reply response: ' + ''.join('{:02x}'
                       .format(x) for x in reply_resp))

    ccu_no = heartbeat.device_details.decode()

    if ccu_no:
        logger.info(f'Preparing to read meters for {ccu_no}')
        read_cmds = get_reading_cmds(ccu_no, redis_params, logger)
        readings = []

        for read_cmd in read_cmds:
            meter, cmd, obis_code = read_cmd
            read = {}
            logger.info(f'Reading {meter} {obis_code} ' + \
                        ''.join('{:02x}'
                                .format(x) for x in obis_code))
            generated_reading_cmd = await generate_reading_cmd(meter, obis_code, logger)
            logger.info('Reading cmd: ' + ''.join('{:02x}'
                                                  .format(x) for x in generated_reading_cmd))
            reading = await get_reading(generated_reading_cmd,
                                        meter,
                                        reader, writer,
                                        logger)
            if reading:
                logger.info(f'Got reading for {meter}: {reading}')
                read['meter_no'] = meter
                read['type'] = cmd
                read['reading'] = reading
                read['timestamp'] = datetime.now().isoformat()
                readings.append(read)

        logger.info('Preparing to send readings')
        await send_readings(logger,
                            hes_server_url,
                            {
                                "data": {
                                    'ccu_no': ccu_no,
                                    'readings': readings
                                }
                            },
                            auth_token)
    else:
        logger.info(f'CCU not found in heartbeat {heartbeat}')
        
    writer.close()


async def get_reading(reading_cmd,
                      meter_no,
                      reader: StreamReader,
                      writer: StreamWriter,
                      logger=None):
    tries = 0
    response = None
    while not response and tries < 3:
        try:
            if logger is not None:
                logger.info("Attempt: %s", tries + 1)
            writer.write(reading_cmd)
            response = await reader.read(100)
            logger.info(f'\nResponse {response} ' + ''.join('{:02x}'
                                            .format(x) for x in response))
            meter_reading = MeterReading(response, logger)
            logger.info(f'\nMeter Reading {meter_reading}')
            return meter_reading.get_value_from_response(meter_no, logger)
        except Exception as e:
            response = None
            logger.info(f'Unable to get reading for {meter_no} Exception: {str(e)}')
            break
        finally:
            tries += 1


async def send_readings(logger, hes_server_url, readings: dict, auth_token):
    """
     Readings format:
     {
       "data":[
         {
          "ccu_no":"MTRK017900013203",
          "readings":[
             {
                 "meter: "017900013203",
                 "type: "phase A voltage",
                 "reading" : "00229.5*V",
                 "timestamp" :"2022-5-27 9:34:49.886451"
             }
            ]
          }
       ]
    }
    """
    res = json.dumps(readings, indent=None, default=str)
    logger.info(f'Sending requests to {hes_server_url}')
    logger.info(f'Sending data {res}')
    resp = requests.post(hes_server_url,
                         data=json.dumps(readings, indent=None, default=str),
                         headers={'Authorization': f'token {auth_token}',
                                  'Content-type': 'application/json'})
    logger.info(f'Send readings response {resp.text}')
    if resp.status_code != 200:
        raise Exception(resp.text)

