import asyncio
import random
import string

from asyncio.streams import StreamWriter, StreamReader
from iec62056_21.messages import CommandMessage


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
    response = await reader.read(100)
    print(f'\n<- Obtained {response}')

    if isinstance(response, (bytes, bytearray)) and \
            response.startswith(b'\x00'):
        # ** processing to get meter data **
        ccu_request = generate_ccu_data_request()
        print(f'\n-> Sending {ccu_request}')
        writer.write(ccu_request)
        await writer.drain()


def generate_ccu_data_request():
    test_read = test_reads('date')
    test_request = b'{"key": "%s", "meter": "MTRK179000229742", "PA": "3", "PASSWORD": "11111111", "RANDOM": 31}|'\
                   + test_read

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


def test_reads(label):
    codes = {
        'voltage': '32.7.0.255',
        'time': '0.9.1.255',
        'date': '0.9.2.255',
    }
    return CommandMessage.for_single_read(codes[label]).to_bytes()


if __name__ == '__main__':
    asyncio.run(start_mock_hes_server())
