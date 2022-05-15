import asyncio


async def read(reader):
    data = bytearray()
    end_chars = []
    buf_size = 1
    timeout = 1000.0
    try:
        while True:
            part = await asyncio.wait_for(reader.read(buf_size), timeout=timeout)
            data += part
            if not part or part in end_chars:
                break
    except asyncio.TimeoutError:
        raise ("timeout reading response after %ss", timeout)
    return data
