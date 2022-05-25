# -*- coding: utf-8 -*-
# Copyright (c) 2021, Manqala and contributors
# For license information, please see license.txt

from __future__ import unicode_literals

from iec62056_21.messages import CommandMessage

from .codes import obis_codes

READ = 'Read'
WRITE = 'Write'


def get_reading_cmds():
    return [
        (cmd,
         CommandMessage.for_single_read(obis_code).to_bytes())
        for cmd, obis_code in obis_codes.items()
    ]


def write_value(obis_code, data):
    cmd = CommandMessage.for_single_write(obis_code, data)
    return cmd.to_bytes()
