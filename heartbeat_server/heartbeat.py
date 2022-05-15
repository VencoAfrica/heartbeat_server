from asyncio.streams import StreamReader, StreamWriter
from datetime import datetime


class Heartbeat:
    def __init__(self, data):
        if isinstance(data, (bytes, bytearray)) and \
               data.startswith(b'\x00'):
            self._data = data
        else:
            raise Exception("Badly formed heartbeat")

    @property
    def data(self):
        return self._data

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
        return (self.version_number +
                self.target_address + 
                self.source_address + 
                self.fixed_format_length + 
                self.fixed_format)

    def parse(self) -> dict:
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
        return not all(not val for val in self.parse().values())

    async def send_heartbeat_reply(self,
                                   writer: StreamWriter,
                                   logger=None):
        reply = self.get_reply()
        if logger:
            logger.info("Sending Server Reply: %s", reply.hex())
        writer.write(reply)

    @staticmethod
    async def read_heartbeat(reader: StreamReader):
        data = bytearray()
        part = await reader.read(8)
        data += part
        frame_length = part[-2:]
        frame_length_int = int.from_bytes(frame_length, 'big')
        part = await reader.read(frame_length_int)
        data += part
        return Heartbeat(data)


