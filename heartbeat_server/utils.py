import socket
import select


async def send(address: str,
               port: int,
               msg: bytes):
    data = bytearray()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((address, port))
    sock.send(msg)

    while True:
        try:
            can_read, _, error = select.select([sock, ], [], [], 5)
            if can_read:
                got = sock.recv(1024)
                print(f'got {got}')
                if got:
                    data += got
            elif error:
                raise select.error(str(error))
            else:
                break
        except select.error as e:
            sock.shutdown(2)
            sock.close()
            break
    return data
