from random import randrange
from logging import Logger

from .meter_device import write_value


DEFAULT_PASSWORD_LEVEL = 0x01
DEFAULT_PASSWORD = b"33333333"
DEFAULT_RANDOM_NUMBER = 31


async def generate_ccu_cmd(meter_no, command_msg, logger: Logger):
    rand = randrange(0, 0xFF-0x33)
    return prep_data(
        meter_no=meter_no,
        pass_lvl=DEFAULT_PASSWORD_LEVEL,
        random_no=rand,
        passw=DEFAULT_PASSWORD,
        data=command_msg,
        logger=logger
    )

async def generate_last_meter_index_cmd(ccu_no, logger):
    get_last_meter_obis = '' # insert obis for last metr
    return generate_ccu_cmd(ccu_no, get_last_meter_obis, logger)

async def generate_add_meter_cmd(meter_no, last_index, logger):
    add_meter_obis = '' # insert obis for add meter (use write command)
    data = '' # generate the data by combining the meter type,
    # meter number and last index
    return generate_ccu_cmd(add_meter_obis,
                            write_value(add_meter_obis, data),
                            logger)

def prep_data(meter_no, pass_lvl, random_no, passw, data, logger: Logger):
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

    # ---
    meter_address = get_meter_no(meter_no)
    _len = length.to_bytes(1, 'big')
    mac_l = get_mac(random_no, passw, 'L')
    mac_h = get_mac(random_no, passw, 'H')
    data = bytearray(0x33 + i for i in data)
    cs = data_sum.to_bytes(2, 'big')[-1:]

    logger.info(f'0x68 meter_address {meter_address} LEN {_len} '
                f'PA {pass_lvl} random {random_no_padded} MAC_L {mac_l} MAC_H {mac_h} '
                f'IEC16056-21 read or write data frames {data} CS {cs} 0x16')
    # ---

    return out


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



