import json
import requests

from asyncio.streams import StreamReader, StreamWriter
from logging import Logger
from datetime import datetime
from .db import Db
from .heartbeat import read_heartbeat
from .hes import generate_ccu_cmd, \
    generate_last_meter_index_cmd, \
    generate_add_meter_cmd, \
    generate_commit_meter_cmd

from .meter_device import get_reading_cmds
from .meter_reading import MeterReading
from .http_requests import HTTPRequest, process_http_request, send_callback, send_response


async def ccu_handler(reader: StreamReader,
                      writer: StreamWriter,
                      hes_server_url: str,
                      redis_params: dict,
                      db_params: dict,
                      hes_auth_token: str,
                      auth_token: str,
                      logger: Logger):
    heartbeat = await read_heartbeat(reader, logger)
    if logger:
        logger.info(f'\nccu.ccu_handler(): Received heartbeat {heartbeat}')

    if isinstance(heartbeat, HTTPRequest):
        try:
            await process_http_request(heartbeat, reader, auth_token,
                                   redis_params, db_params, writer, logger)
        except Exception as e:
            await send_response(writer, 0, 400, str(e), logger)
        finally:
            writer.close()
    else:
        reply_resp = await heartbeat.send_heartbeat_reply(logger, reader, writer)
        logger.info('Heartbeat reply response: ' + ''.join('{:02x}'
                                                           .format(x) for x in reply_resp))

        ccu_no = heartbeat.device_details.decode()
        if ccu_no:
            logger.info(f'Preparing to read meters for {ccu_no}')
            remote_commands = get_reading_cmds(ccu_no, db_params ,redis_params, logger)
            read_cmds = remote_commands[0]
            readings = []

            for add_meter_command in remote_commands[1]:
                # processing only add meter commands
                meter_no, cmd, obis_code, callback_url, request_id = add_meter_command
                ccu_no, meter = meter_no.split("-")
                read = {}
                logger.info(f'Reading {meter} {obis_code} ' + \
                            ''.join('{:02x}'
                                    .format(x) for x in obis_code))
                logger.info('Reading cmd: ' + ''.join('{:02x}'
                                                      .format(x) for x in generated_reading_cmd))

                try:
                    generated_reading_cmd = generate_last_meter_index_cmd(ccu_no, logger)
                    reading = await get_ccu_command_result(generated_reading_cmd,
                                                           ccu_no,
                                                           reader, writer,
                                                           logger)
                    if reading:
                        logger.info(f'Got last meter index for {ccu_no}: {reading}')
                        last_index = reading
                        add_meter_command = generate_add_meter_cmd(ccu_no, meter, last_index, logger)
                        data = await get_ccu_command_result(add_meter_command,
                                                           ccu_no,
                                                           reader, writer,
                                                           logger)
                        if data:
                            logger.info(f'Added meter {meter} for {ccu_no}: {data}')
                            commit_meter_command = generate_commit_meter_cmd(ccu_no, last_index, logger)
                            commit_data = await get_ccu_command_result(commit_meter_command,
                                                                        ccu_no,
                                                                        reader, writer,
                                                                        logger)
                            if commit_data:
                                logger.info(f'Commited {meter} index for {ccu_no}: {reading}')
                                heartbeat_name = db_params.get('name')
                                heartbeat_db = Db(heartbeat_name)
                                heartbeat_db.add(ccu_no, meter)
                    send_response(200, commit_data, meter, request_id, logger, callback_url)
                except Exception as e:
                    send_response(500, str(e), meter, request_id, logger, callback_url)
                    logger.info(f'Error obtaining adding meter {meter} to ccu {ccu_no}')                      

            for read_cmd in read_cmds:
                meter, cmd, obis_code, callback_url, request_id = read_cmd
                read = {}
                logger.info(f'Reading {meter} {obis_code} ' + \
                            ''.join('{:02x}'
                                    .format(x) for x in obis_code))
                generated_reading_cmd = await generate_ccu_cmd(meter, obis_code, logger)
                logger.info('Reading cmd: ' + ''.join('{:02x}'
                                                      .format(x) for x in generated_reading_cmd))

                try:
                    reading = await get_ccu_command_result(generated_reading_cmd,
                                                           meter,
                                                           reader, writer,
                                                           logger)
                    if reading:
                        logger.info(f'Got reading for {meter}: {reading}')
                        curr_timestamp = datetime.now().isoformat()
                        read['meter_no'] = meter
                        read['type'] = cmd
                        read['reading'] = reading
                        read['timestamp'] = curr_timestamp
                        readings.append(read)
                        send_response(200, reading, meter, request_id, logger, callback_url)
            
                except Exception as e:
                    send_response(500, str(e), meter, request_id, logger, callback_url)
                    logger.info(f'Error obtaining reading for {meter}')
        
            logger.info('Preparing to send readings')
            await send_readings(logger,
                                hes_server_url,
                                {
                                    "data": {
                                        'ccu_no': ccu_no,
                                        'readings': readings
                                    }
                                },
                                hes_auth_token)
        else:
            logger.info(f'CCU not found in heartbeat {heartbeat}')

        writer.close()

def send_response(status, message, meter_no, request_id, logger, callback_url=None):
    if callback_url:
        data = {
            'status': status,
            'message': message,
            'meter_no': meter_no,
            'request_id': request_id,
            'timestamp':  datetime.now().isoformat()
        }
        send_callback(data, callback_url, logger)


async def get_ccu_command_result(reading_cmd,
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
            msg = f'Unable to get reading for {meter_no} Exception: {str(e)}'
            logger.info(msg)
            raise Exception(msg)
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


