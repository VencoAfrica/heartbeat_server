from __future__ import unicode_literals

from iec62056_21.messages import CommandMessage

from .codes import obis_codes
from .db import Db
from logging import Logger

import redis

READ = 'Read'
WRITE = 'Write'
REMOTE_REQUEST_DELIMITER = '*|'


def get_reading_cmds(ccu_no, db_params, redis_params, logger: Logger):
    db_name = db_params.get('name')
    db = Db(db_name)
    meters = db.get_meters(ccu_no)
    NO_CALLBACK_URL = None
    REQUEST_ID = None
    read_commands = [
        (
            meter,
            cmd,
            read_value(obis_code),
            NO_CALLBACK_URL,
            REQUEST_ID
        )
        for cmd, obis_code in obis_codes.items()
        for meter in meters
    ]
    remote_commands = get_remote_request_commands(redis_params, meters, logger)
    add_meter_remote_commands = remote_commands[1]
    commands = read_commands + remote_commands[0]
    logger.info(f'Generated commands {commands}')
    return commands, add_meter_remote_commands


def write_value(obis_code, data):
    cmd = CommandMessage.for_single_write(obis_code, data)
    return cmd.to_bytes()


def read_value(obis_code):
    cmd = CommandMessage.for_single_read(obis_code)
    return cmd.to_bytes()


def get_remote_request_commands(redis_params, meters:list, logger: Logger):
    """
    Remote requests may be reads or writes and should
    have the following format

    Write
    ------
    key: *|<timestamp>
    value: W:meter_no:cmd:OBIS_Code:value:callback_url

    Read
    -----
    key: *|<timestamp>
    value: R:meter_no:cmd:OBIS_Code:callback_url

    """
    remote_commands = []
    remote_add_meter_commands = []
    r = redis.Redis(host=redis_params.get('host', '0.0.0.0'),
                    port=redis_params.get('port', 6379),
                    db=redis_params.get('db', 0))
    for key in r.scan_iter("*"):
        try:
            logger.info(f'Processing redis key {key}')
            if str(key, 'utf-8') \
                    .startswith(REMOTE_REQUEST_DELIMITER):
                command = str(r.get(key), 'utf-8').split(REMOTE_REQUEST_DELIMITER)
                mode = command[0]
                meter_no = command[1]
                logical_command = command[2]
                obis_code = command[3]
                callback_url = command[-1]

                if obis_code == "96.51.90.255":
                    request_id = str(key, 'utf-8').split(REMOTE_REQUEST_DELIMITER)[1]
                    logger.info(f'Request id {request_id}')
                    
                    remote_add_meter_commands.append(
                        meter_no,
                        logical_command,
                        read_value(obis_code),
                        callback_url,
                        request_id
                    )
                else:
                    request_id = str(key, 'utf-8').split(REMOTE_REQUEST_DELIMITER)[1]
                    logger.info(f'Request id {request_id}')

                    if meter_no in meters:
                        if mode.upper() == 'W':
                            value = command[4]
                            remote_commands.append(
                                (
                                    meter_no,
                                    logical_command,
                                    write_value(obis_code, value),
                                    callback_url,
                                    request_id
                                )
                            )
                        elif mode.upper() == 'R':
                            remote_commands.append(
                                (
                                    meter_no,
                                    logical_command,
                                    read_value(obis_code),
                                    callback_url,
                                    request_id
                                )
                            )
                        r.delete(key)
        except Exception as e:
            logger.error(f'Error processing redis key {key} {e}')
            continue

    logger.info(f'Remote Commands {remote_commands}')
    return remote_commands, remote_add_meter_commands
