import asyncio
import random
import string

from asyncio.streams import StreamWriter, StreamReader
from test_utils import read


async def start_mock_hes_server():
    server = await asyncio.start_server(
        handle_heartbeat_server,
        '0.0.0.0',
        18902
    )

    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Running test HES on {addrs}')

    async with server:
        await server.serve_forever()


async def handle_heartbeat_server(reader: StreamReader,
                                  writer: StreamWriter):
    response = await read(reader)
    print(f'Obtained heartbeat {response}')

    # bytearray(b'\x00\x01\x00\x01\x00f\x00\x1e\x0f\xc0\x00\x00\x00\x02\x02\n\x10MTRK017900013203\x06\x00\x00\x00\x00')
    if isinstance(response, (bytes, bytearray)) and \
            response.startswith(b'\x00'):
        # processing to get meter data
        ccu_request = generate_ccu_data_request(writer)
        print(f'Sending to Heart beat server {ccu_request}')
        writer.write(ccu_request)

    writer.close()
    # elif ():  # reading
    #     pass


def generate_ccu_data_request(writer: StreamWriter):
    test_request = b'{"key": "%s", "meter": "179000222382", "PA": "3", "PASSWORD": "11111111", "RANDOM": ' \
                   b'31}|\x01R1\x020.9.2.255()\x03D '

    key = generate_key()
    key_bytes = bytes(key, encoding='utf-8')
    return test_request % key_bytes


def generate_key():
    return ''.join(
        [
            random.choice(string.ascii_letters + string.digits) \
            for _ in range(20)
        ]
    )


if __name__ == '__main__':
    asyncio.run(start_mock_hes_server())
