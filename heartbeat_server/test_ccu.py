import asyncio

from asyncio.streams import StreamWriter, StreamReader
from .test_utils import read


async def handle_heartbeat_server(reader: StreamReader,
                                  writer: StreamWriter):
    response = await read(reader)
    print(f'Obtained response {response}')


async def start_mock_ccu():
    server = await asyncio.start_server(
        handle_heartbeat_server,
        '0.0.0.0',
        18903
    )

    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Running test HES on {addrs}')

    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    asyncio.run(start_mock_ccu())
    print('here')
