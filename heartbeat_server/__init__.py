import asyncio
import json
import os
from heartbeat_server.logger import get_logger
from heartbeat_server.parser import HeartbeartData


def _load_config(filename="config.json"):
    logger = get_logger()
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f)
    else:
        logger.error("Failed to load configuration files ('%s' Not Found)" % filename)


def _parse_heartbeat(in_data):
	try:
		parsed = HeartbeartData(in_data)

		print('version_number: ', bytearray(parsed.version_number).hex())
		print('source_address: ', bytearray(parsed.target_address).hex())
		print('target_address: ', bytearray(parsed.source_address).hex())
		print('frame_length: ', bytearray(parsed.out_fixed_format_length).hex())
		print('fixed_format: ', bytearray(parsed.out_fixed_format).hex())

        out_data = parsed.output_data
		print('heartbeat response: ', bytearray(out_data).hex())
	except IndexError as err:
		print("badly formed heartbeat. cannot log or respond!")
		print(err)
		out_data = b'\xFF'
		
	return out_data

async def server_handler(reader, writer):
    logger = get_logger()

    data = bytearray()
    while True:
        part = await reader.read(100)
        data += part
        if not part or part.endswith((b'\n', b'\r', b'\r\n')):
            break

    logger.info("Received %s", data)

    out_data = _parse_heartbeat(data)

    logger.info("Parsed %s", data)

    writer.close()

async def main(config):
    host = config.get('host', '127.0.0.1')
    port = config.get('port', '18901')

    server = await asyncio.start_server(server_handler, host, port)
    addr = server.sockets[0].getsockname()
    print('Serving on ', addr)

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    # load config and run server
    config = _load_config
    asyncio.run(server(config))
