import socket
import select


async def send(address: str,
               port: int,
               msg: bytes):
    got = None

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((address, port))
    sock.send(msg)

    while True:
        try:
            can_read, _, error = select.select([sock, ], [], [], 5)
            if can_read:
                got = sock.recv(1024)
                print(f'got %s', got)
            if not can_read:
                break
            if error:
                raise select.error(str(error))
        except select.error as e:
            sock.shutdown(2)
            sock.close()
            break
    return got
