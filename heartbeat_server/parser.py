import asyncio
from asyncio.streams import StreamReader
from datetime import datetime
from logging import Logger


class HeartbeartData:
    def __init__(self, data):
        self._data = data
    
    @property
    def version_number(self):
        return self._data[0:2]

    @property
    def source_address(self):
        return self._data[2:4]

    @property
    def target_address(self):
        return self._data[4:6]

    @property
    def frame_length(self):
        return self._data[6:8]

    @property
    def fixed_format(self):
        return self._data[8:13]

    @property
    def fixed_format_length(self):
        return len(self.fixed_format).to_bytes(2, 'big')

    @property
    def structure(self):
        return self._data[13:15]

    @property
    def visible_string(self):
        return self._data[15:16]

    @property
    def device_details_length(self):
        return self._data[16:17]

    @property
    def device_details_length_int(self):
        return int.from_bytes(self.device_details_length, byteorder='big')

    @property
    def device_details(self):
        return self._data[17: 17 + self.device_details_length_int]

    @property
    def double_long_unsigned(self):
        return self._data[17 + self.device_details_length_int:
                          17 + self.device_details_length_int + 1]

    @property
    def address(self):
        return self._data[17 + self.device_details_length_int + 1:
                          17 + self.device_details_length_int + 5]

    @property
    def output_data(self):
        ''' create out_data remember to transports the target_address and source_address '''
        return (self.version_number + 
                self.target_address + 
                self.source_address + 
                self.fixed_format_length + 
                self.fixed_format)
    
    def get_parsed(self):
        ''' parse data to be put in queue. Can raise exception '''
        return {
            "version_number": self.version_number.hex(),
            "source_address": self.source_address.hex(),
            "target_address": self.target_address.hex(),
            "frame_length": self.frame_length.hex(),
            "fixed_format": self.fixed_format.hex(),
            "device_ip": self.address.decode(),
            "device_details": self.device_details.decode(),
            "timestamp": datetime.now().timestamp()
        }

    def get_reply(self):
        frame_length_size = 2
        frame_length = len(self.fixed_format)
        return b''.join([
            self.version_number,
            self.target_address,
            self.source_address,
            frame_length.to_bytes(frame_length_size, 'big'),
            self.fixed_format
        ])

    def is_valid(self):
        if not isinstance(self._data, (bytes, bytearray)):
            return False

        if not self._data.startswith(b'\x00'):
            return False

        try:
            for val in self.get_parsed().values():
                if not val:
                    return False
        except:
            return False

        return True

    @staticmethod
    async def read_heartbeat(reader: StreamReader, logger: Logger = None):
        out = []
        while True:
            data = bytearray()
            try:
                part = await asyncio.wait_for(reader.read(8), timeout=3.0)
                if len(part) != 8:
                    break
                data += part
                frame_length = part[-2:]
                frame_length_int = int.from_bytes(frame_length, 'big')
                part = await asyncio.wait_for(
                    reader.read(frame_length_int), timeout=3.0)
                data += part
                out.append(data)
            except asyncio.TimeoutError:
                if logger is not None:
                    logger.exception("Timeout reading heartbeat")
                if data:
                    out.append(data)
                break
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

def get_mac(password, low_or_high='L'):
    if not isinstance(password, (bytes, bytearray)):
        password = bytes(password,'utf-8')

    arr = bytearray()
    if low_or_high == 'L':
        arr += b'\x1F' + password
    elif low_or_high == 'H':
        arr += password + b'\x1F'
    else:
        raise TypeError('low_or_high should be "H" or "L"')
    crc = CRC(b'\xA5', arr)
    return (int.from_bytes(crc, 'big') + 0x33).to_bytes(2, 'big')[-1:]

def prep_data(meter_no, pass_lvl, random_no, passw, data):
	if not isinstance(pass_lvl, bytes):
		pass_lvl = (int(str(pass_lvl)) + 0x33).to_bytes(1, 'big')

	if not isinstance(random_no, bytes):
		random_no = (int(str(random_no)) + 0x33).to_bytes(1, 'big')

	# 77 77 PA 52 MAC_L MAC_H, length = 6
	length = 6 + len(data)
	out = bytearray([0x68])
	out += get_meter_no(meter_no)
	out += bytearray([0x68, 0x01])
	out += length.to_bytes(1, 'big')
	out += bytearray([0x77, 0x77])
	out += pass_lvl
	out += random_no
	out += get_mac(passw, 'L')
	out += get_mac(passw, 'H')
	out += bytearray(0x33 + i for i in data)

	data_sum = sum(out)
	out += data_sum.to_bytes(2, 'big')[-1:]
	out += bytearray([0x16])
	return out
