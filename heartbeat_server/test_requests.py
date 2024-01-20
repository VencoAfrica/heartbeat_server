import select
import socket
import threading
import time

def simulate_ccu_heartbeat():
    hs_address = '18.117.213.18'
    hs_port = 15870

    heartbeat = b'\x00\x01\x00\x01\x00\x66\x00\x1E\x0F\xC0\x00\x00\x00\x02\x02\x0A\x10\x4D\x54\x52\x4B\x30\x31\x37\x39\x30\x30\x30\x31\x33\x32\x30\x33\x06\x00\x00\x00\x00'
    reading = b'\x68\x03\x32\x01\x00\x79\x01\x68\x81\x10\x77\x77\x35\x5b\x63\x63\x65\x65\x6c\x61\x68\x5d\x89\x5c\x36\x8f\x4d\x16'

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((hs_address, hs_port))

    while True:
        try:
            print("Before sending heartbeat")
            sock.send(heartbeat)
            print("After sending heartbeat")
            time.sleep(180)  # Sleep for 3 minutes

            can_read, _, error = select.select([sock, ], [], [], 5)
            if can_read:
                got = sock.recv(1024)

                if got.startswith(b'\x00'):
                    print(f'\ntest_requests.simulate_ccu_heartbeat: Received {got}')
                elif got.startswith(b'\x68'):
                    print(f'\ntest_requests.simulate_ccu_heartbeat: Received {got}')
                    print(f'\ntest_requests.simulate_ccu_heartbeat: Sending {heartbeat}')
                    sock.send(reading)
                if not got:
                    break
            if error:
                raise select.error(str(error))
        except select.error as e:
            sock.shutdown(2)
            sock.close()
            break

if __name__ == '__main__':
    try:
        while True:
            simulate_ccu_heartbeat()
    except Exception as e:
        print(f"Exception: {str(e)}")