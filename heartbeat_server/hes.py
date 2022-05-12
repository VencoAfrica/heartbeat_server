import json
import asyncio
from .redis import Redis
from . import Heartbeat

from logging import Logger
from asyncio.streams import StreamReader
from iec62056_21.messages import CommandMessage

DEFAULT_PASSWORD_LEVEL = 0x01
DEFAULT_PASSWORD = b"33333333"
DEFAULT_RANDOM_NUMBER = 31


async def hes_handler(reader, writer, res,
                      sub_redis: Redis):
    split = res[1].split(b'|', 1) if res else []
    aux, data = json.loads(split[0]), split[1]
    key = aux['key']
    to_send = prep_data_from_aux(
        aux=aux, meter_no=aux['meter'], data=data
    )
    response = await send_data(to_send, reader, writer)
    await sub_redis.publish('RESPONSE: {}|{}'.format(key, response))


def prep_data_from_aux(aux, meter_no, data):
    password_level = aux.get("PA", DEFAULT_PASSWORD_LEVEL)
    password = aux.get("PASSWORD", DEFAULT_PASSWORD)
    random_number = aux.get("RANDOM", DEFAULT_RANDOM_NUMBER)

    return prep_data(
        meter_no=meter_no,
        pass_lvl=password_level,
        random_no=random_number,
        passw=password,
        data=data
    )


def prep_data_from_config(config, meter_no, data):
    password_level = config.get("password_level", DEFAULT_PASSWORD_LEVEL)
    password = config.get("password", DEFAULT_PASSWORD)
    random_number = config.get("random_number", DEFAULT_RANDOM_NUMBER)

    return prep_data(
        meter_no=meter_no,
        pass_lvl=password_level,
        random_no=random_number,
        passw=password,
        data=data
    )


def prep_data(meter_no, pass_lvl, random_no, passw, data):
    if not isinstance(random_no, bytes):
        random_no = (int(str(random_no))).to_bytes(1, 'big')

    if not isinstance(pass_lvl, bytes):
        pass_lvl = (int(str(pass_lvl)) + 0x33).to_bytes(1, 'big')

    random_no_padded = (int.from_bytes(
        random_no, 'big') + 0x33).to_bytes(1, 'big')

    # 77 77 PA 52 MAC_L MAC_H, length = 6
    length = 6 + len(data)
    out = bytearray([0x68])
    out += get_meter_no(meter_no)
    out += bytearray([0x68, 0x01])
    out += length.to_bytes(1, 'big')
    out += bytearray([0x77, 0x77])
    out += pass_lvl
    out += random_no_padded
    out += get_mac(random_no, passw, 'L')
    out += get_mac(random_no, passw, 'H')
    out += bytearray(0x33 + i for i in data)

    data_sum = sum(out)
    out += data_sum.to_bytes(2, 'big')[-1:]
    out += bytearray([0x16])
    return out


async def test_reads(reader, writer, meter, logger=None, codes=None):
    codes = codes or [
        ('voltage', '32.7.0.255'), ('time', '0.9.1.255'),
        ('date', '0.9.2.255'),
    ]
    for label, code in codes:
        msg = CommandMessage.for_single_read(code).to_bytes()
        PA, password = DEFAULT_PASSWORD_LEVEL, DEFAULT_PASSWORD
        to_send = prep_data(
            meter, PA, DEFAULT_RANDOM_NUMBER, password, msg
        )
        if logger:
            logger.info(
                "Sending Data [%s %r]: %s", label, (PA, password), to_send.hex())
        try:
            response = await send_data(to_send, reader, writer, logger)
            if logger:
                logger.info(
                    "write response [%s %r]: %s", label, (PA, password), response.hex())
        except BrokenPipeError:
            writer.close()
            if logger:
                logger.exception("broken pipe on time read")


async def send_data(data, reader, writer, logger=None):
    tries = 0
    response = bytearray()
    while not response and tries < 3:
        if logger is not None:
            logger.info("Trying: %s", tries + 1)
        await asyncio.sleep(1)
        writer.write(data)

        await asyncio.sleep(1)
        response = await read_response(reader, logger=logger, timeout=3.0)
        heartbeat = Heartbeat(response)
        if heartbeat.is_valid():
            await Heartbeat.send_heartbeat_reply(heartbeat, writer, logger)
            response = bytearray()
        tries += 1
    return response


def CRC(crc, buf):
    CRC8Tbl = bytearray([
        0x00, 0x31, 0x62, 0x53, 0xC4, 0xF5, 0xA6, 0x97,
        0xB9, 0x88, 0xDB, 0xEA, 0x7D, 0x4C, 0x1F, 0x2E
    ])
    crc_int = int.from_bytes(crc, 'big')
    for b in buf:
        tmp = crc_int >> 4
        crc_int = (crc_int << 4) & 255
        crc_int ^= CRC8Tbl[tmp ^ (b >> 4)]
        tmp = crc_int >> 4
        crc_int = (crc_int << 4) & 255
        crc_int ^= CRC8Tbl[tmp ^ (b & 0x0F)]
    return crc_int.to_bytes(1, 'big')


def get_meter_no(meter):
    meter = ''.join(ch for ch in meter if ch.isdecimal())
    n = len(meter)
    meter_reversed = [meter[i-2: n+i] for i in range(0, -n, -2)]
    return bytearray(int(i, 16) for i in meter_reversed)


def get_mac(random_no, password, low_or_high='L'):
    if not isinstance(random_no, bytes):
        random_no = (int(str(random_no))).to_bytes(1, 'big')

    if not isinstance(password, (bytes, bytearray)):
        password = bytes(password,'utf-8')

    arr = bytearray()
    if low_or_high == 'L':
        arr += random_no + password
    elif low_or_high == 'H':
        arr += password + random_no
    else:
        raise TypeError('low_or_high should be "H" or "L"')
    crc = CRC(b'\xA5', arr)
    return (int.from_bytes(crc, 'big') + 0x33).to_bytes(2, 'big')[-1:]


async def read_response(reader: StreamReader, end_chars=None,
                        buf_size=1, timeout=10.0, logger: Logger = None):
    data = bytearray()
    end_chars = end_chars if end_chars is not None else []
    try:
        while True:
            part = await asyncio.wait_for(reader.read(buf_size), timeout=timeout)
            data += part
            if not part or part in end_chars:
                break
    except asyncio.TimeoutError:
        if logger is not None:
            logger.exception("timeout reading response after %ss", timeout)
    return data
