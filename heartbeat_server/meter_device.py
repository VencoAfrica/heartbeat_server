# -*- coding: utf-8 -*-
# Copyright (c) 2021, Manqala and contributors
# For license information, please see license.txt
from __future__ import unicode_literals

from iec62056_21.messages import CommandMessage

from .codes import obis_codes

import redis

READ = 'Read'
WRITE = 'Write'


def get_reading_cmds(redis_params):
    read_commands = [
        (cmd,
         CommandMessage.for_single_read(obis_code).to_bytes())
        for cmd, obis_code in obis_codes.items()
    ]
    return [read_commands,
            get_write_commands(
                redis_params)]


def write_value(obis_code, data):
    cmd = CommandMessage.for_single_write(obis_code, data)
    return cmd.to_bytes()


def get_write_commands(redis_params):
    write_commands = []
    r = redis.Redis(host=redis_params.get('host', '0.0.0.0'),
                    port=redis_params.get('port', 6379),
                    db=redis_params.get('db', 0))
    for key in r.scan_iter("*"):
        command = r.get(key)
        write_commands.append(
            write_value(command.split(':')[0],
                        command.split(':')[1]))
    return write_commands
