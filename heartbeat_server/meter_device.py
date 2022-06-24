# -*- coding: utf-8 -*-
# Copyright (c) 2021, Manqala and contributors
# For license information, please see license.txt
import redis

from __future__ import unicode_literals

from iec62056_21.messages import CommandMessage

from .codes import obis_codes

READ = 'Read'
WRITE = 'Write'


def get_reading_cmds():
    read_commands = [
        (cmd,
         CommandMessage.for_single_read(obis_code).to_bytes())
        for cmd, obis_code in obis_codes.items()
    ]
    return [read_commands, get_write_commands()]


def write_value(obis_code, data):
    cmd = CommandMessage.for_single_write(obis_code, data)
    return cmd.to_bytes()


def get_write_commands():
    write_commands = []
    r = redis.Redis(host='localhost',
                    port=6379,
                    db=0)
    for key in r.scan_iter("*"):
        command = r.get(key)
        write_commands.append(
            write_value(command.split(':')[0],
                        command.split(':')[1]))
    return write_commands
