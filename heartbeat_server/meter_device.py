from __future__ import unicode_literals

from iec62056_21.messages import CommandMessage

from .codes import obis_codes
from .meters import ccu_meters
from logging import Logger

import redis

READ = 'Read'
WRITE = 'Write'
REMOTE_REQUEST_DELIMITER = '*|'


def get_reading_cmds(ccu_no, redis_params, logger: Logger):
    read_commands = [
        (
            meter,
            cmd,
            read_value(obis_code)
        )
        for cmd, obis_code in obis_codes.items()
        for meter in ccu_meters.get(ccu_no)
    ]
    commands = read_commands + get_remote_request_commands(redis_params, logger)
    logger.info(f'Generated commands {commands}')
    return commands


def write_value(obis_code, data):
    cmd = CommandMessage.for_single_write(obis_code, data)
    return cmd.to_bytes()


def read_value(obis_code):
    cmd = CommandMessage.for_single_read(obis_code)
    return cmd.to_bytes()


def get_remote_request_commands(redis_params, logger: Logger):
    """
    Remote requests may be reads or writes and should
    have the following format

    Write:
    ------
    key: *|<timestamp>
    value: W:meter_no:cmd:OBIS_Code:value

    Read:
    -----
    key: *|<timestamp>
    value: R:meter_no:cmd:OBIS_Code

    Examples:
    --------
    set *|1663672448 "R:MTRK179000989931:Get Meters Count:96.51.90.255"
    set *|1663672449 "R:MTRK179000989931:Get First Meter:96.51.91.1"
    """
    remote_commands = []
    r = redis.Redis(host=redis_params.get('host', '0.0.0.0'),
                    port=redis_params.get('port', 6379),
                    db=redis_params.get('db', 0))
    for key in r.scan_iter("*"):
        logger.info(f'Processing redis key {key}')
        if str(key, 'utf-8') \
                .startswith(REMOTE_REQUEST_DELIMITER):
            command = str(r.get(key), 'utf-8').split(':')
            mode = command[0]
            meter_no = command[1]
            logical_command = command[2]
            obis_code = command[3]

            if mode.upper() == 'W':
                value = command[4]
                remote_commands.append(
                    (
                        meter_no,
                        logical_command,
                        write_value(obis_code, value)
                    )
                )
            elif mode.upper() == 'R':
                remote_commands.append(
                    (
                        meter_no,
                        logical_command,
                        read_value(obis_code)
                    )
                )
            r.delete(key)
    logger.info(f'Remote Commands {remote_commands}')
    return remote_commands
