import json
import requests

from asyncio.streams import StreamReader, StreamWriter
from logging import Logger
from datetime import datetime

from .heartbeat import read_heartbeat
from .hes import generate_reading_cmd

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
    await register_ccu(heartbeat)
    if logger:
        logger.info(f'\nccu.ccu_handler(): Received heartbeat {heartbeat}')

    if isinstance(heartbeat, HTTPRequest):
        try:
            await process_http_request(heartbeat, reader, auth_token,
                                   redis_params, db_params, writer)
            await send_response(writer, 0, 200, "request successful")
        except Exception as e:
            await send_response(writer, 0, 400, str(e))
        finally:
            writer.close()
    else:
        reply_resp = await heartbeat.send_heartbeat_reply(logger, reader, writer)
        logger.info('Heartbeat reply response: ' + ''.join('{:02x}'
                                                           .format(x) for x in reply_resp))

        ccu_no = heartbeat.device_details.decode()

        if ccu_no:
            #logger.info(f'Preparing to read meters for {ccu_no}')
            read_cmds = get_reading_cmds(ccu_no, db_params ,redis_params, logger)
            readings = []

            for read_cmd in read_cmds:
                meter, cmd, obis_code, callback_url, request_id = read_cmd
                read = {}
                #logger.info(f'Reading {meter} {obis_code} ' + \
                #            ''.join('{:02x}'
                #                    .format(x) for x in obis_code))
                generated_reading_cmd = await generate_reading_cmd(meter, obis_code, logger)
                #logger.info(f'Reading cmd: {callback_url}' + ''.join('{:02x}'
                #                                      .format(x) for x in generated_reading_cmd))

                try:
                    reading = await get_reading(generated_reading_cmd,
                                                meter,
                                                reader, writer,
                                                logger)
                    if reading:
                        #logger.info(f'Got reading for {meter}: {reading}')
                        curr_timestamp = datetime.now().isoformat()
                        read['meter_no'] = meter
                        read['type'] = cmd
                        read['reading'] = reading
                        read['timestamp'] = curr_timestamp
                        readings.append(read)

                        if callback_url:
                            data = {
                                'status': 200,
                                'message': reading,
                                'meter_no': meter,
                                'request_id': request_id,
                                'timestamp':  curr_timestamp
                            }
                            send_callback(data, callback_url, logger)

                except Exception as e:
                    logger.info(f'Error obtaining reading for {meter}')
                    if callback_url:
                        data = {
                            'status': 500,
                            'message': str(e),
                            'meter_no': meter,
                            'request_id': request_id,
                            'timestamp': datetime.now().isoformat()
                        }
                        send_callback(data, callback_url, logger)

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
            response = await reader.read(200)
            logger.info(f'\nXResponse {meter_no} {response} ' + ''.join('{:02x}'
                                            .format(x) for x in response))
            meter_reading = MeterReading(response, logger)
            #logger.info(f'\nMeter Reading {meter_reading}')
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
                 "meter_no: "017900013203",
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
    log_readings(readings.get('data'), logger)
    resp = requests.post('https://meterservices.venco.africa/api/method/meter_services.v1.log_readings',
        data=json.dumps(readings, indent=None, default=str),
        headers={'Authorization': f'token c0d9a22d1b344da:7490dd75996ede3',
        'Content-type': 'application/json'})
    logger.info(f'Send readings response {resp.text}')
    if resp.status_code != 200:
        raise Exception(resp.text)
    
def log_readings(data, logger):
    from .mongodb import MongoDb
    try:
        heartbeat_db = MongoDb('heartbeat')
        readings = []
        today = datetime.today()
        ccu_no = data['ccu_no']
        _readings = data['readings']
        _meter_numbers = []
        _reading_types = []
        for i in _readings:
            meter_number = i.get('meter_no')
            reading_type = i.get('type')
            reading = i.get('reading')
            timestamp = i.get('timestamp')
            readings.append({
                'meter_number': meter_number,
                'ccu': ccu_no,
                'type': reading_type,
                'reading': reading,
                'timestamp': datetime.strptime(timestamp,'%Y-%m-%d %H:%M:%S.%f')
            })

        heartbeat_db.delete_meter_readings({
            'meter_number':  {"$in": list(set(_meter_numbers))},
            'ccu': ccu_no,
            'type': list(set(_reading_types)),
            'timestamp': {'$gte', today}
        })
        heartbeat_db.add_meter_readings(readings)
    except:
        logger.exception("MongoDB:Log Readings Error")

async def register_ccu(heartbeat):
    try:
        ccu_no = heartbeat.device_details
        resp = requests.post('https://meterservices.venco.africa/api/method/meter_services.v1.add_ccu',
            data=json.dumps({'ccu_no': ccu_no.decode()}, indent=None, default=str),
            headers={'Authorization': f'token c0d9a22d1b344da:7490dd75996ede3','Content-type': 'application/json'})
    except:
        pass
