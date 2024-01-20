from iec62056_21.messages import AnswerDataMessage
from logging import Logger

class MeterReading:
    def __init__(self, data, logger):
        if isinstance(data, (bytes, bytearray)) and \
               data.startswith(b'\x68'):
            self._data = data
        else:
            error_msg = 'Badly formed reading ' + ''.join('{:02x}'
                             .format(x) for x in data)
            logger.info(error_msg)
            raise Exception(error_msg)

    @property
    def data(self):
        return self._data

    def get_value_from_response(self, meter_no, logger: Logger):
        resp = self.prep_response(self._data, meter_no)
        try:
            answer = AnswerDataMessage.from_bytes(resp)
            #logger.info('XReading answer type >')
            #logger.info(type(answer))
            #logger.info(answer.data)
            #logger.info(resp.hex())
            #logger.info('XReading answer type <')
            result = ''
            # answer_str = answer.decode('utf-8')
            # logger.info(f'IEC21 return data {answer_str}')
            for data in answer.data:
                logger.info(f'{data.value}')
                if data.value:
                    result += data.value + ' '
                else:
                    result += '()'
            # logger.info(f'Result {result}')
            # logger.info(f'Succeeded to parse response:_data {self._data}')
            # logger.info(f'Succeeded to parse response:resp {resp}')
            # logger.info(f'Succeeded to parse response:result {result}')
            return result
        except Exception as e:
            logger.info(f'Raw meter number :{meter_no}')
            # logger.exception(f'Failed to parse response: {str(e)}')
            logger.info(f'Failed to parse response:_data {self._data}')
            logger.info(f'Failed to parse response:resp {resp}')
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
