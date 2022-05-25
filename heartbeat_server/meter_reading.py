import binascii

from iec62056_21.messages import AnswerDataMessage


class MeterReading:
    def __init__(self, data):
        if isinstance(data, (bytes, bytearray)) and \
               data.startswith(b'\x68'):
            self._data = data
        else:
            raise Exception("Badly formed reading")

    @property
    def data(self):
        return self._data

    def get_value_from_response(self, meter_no):
        if not isinstance(self._data, (bytes, bytearray)):
            reading = binascii.unhexlify(self._data)

        if not isinstance(meter_no, str):
            raise TypeError("Please pass 'meter_no' as a string (got %r)" % (meter_no,))

        resp = self.prep_response(reading, meter_no)
        try:
            answer = AnswerDataMessage.from_bytes(resp)
            for data in answer.data:
                if data.value:
                    return data.value
                return resp.hex()
        except:
            return resp.hex()

    def prep_response(self, response, meter_no):
        out = []
        head = bytearray([0x68]) + self.get_meter_no(meter_no) + bytearray([0x68])
        parts = response.split(head)
        for part in parts:
            if not part:
                continue
            if part[0] in [0x81, 0xC1]:
                start = 4 if part[0] == 0x81 else 2
                try:
                    out.append(bytearray(i - 0x33 for i in part[start:-2]))
                except Exception as e:
                    pass
        return out[-1] if out else response

    def get_meter_no(self, meter):
        meter = ''.join(ch for ch in meter if ch.isdecimal())
        n = len(meter)
        meter_reversed = [meter[i - 2: n + i] for i in range(0, -n, -2)]
        return bytearray(int(i, 16) for i in meter_reversed)
