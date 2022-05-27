import select
import socket
import threading
import time

no_of_heartbeats = 1000


def simulate_ccu_heartbeat():
    start = time.time()

    heartbeat = b'\x00\x01\x00\x01\x00\x66\x00\x1E\x0F\xC0\x00\x00\x00\x02\x02\x0A\x10\x4D\x54\x52\x4B\x30\x31\x37\x39\x30\x30\x30\x31\x33\x32\x30\x33\x06\x00\x00\x00\x00'

    reading = b'\x68\x03\x32\x01\x00\x79\x01\x68\x81\x10\x77\x77\x35\x5b\x63\x63\x65\x65\x6c\x61\x68\x5d\x89\x5c\x36\x8f\x4d\x16'
# 'MTRK017900013203'
    # \x03\x32\x01\x00\x79\x01
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((hs_address, hs_port))
    sock.send(heartbeat)

    while True:
        try:
            can_read, _, error = select.select([sock, ], [], [], 5)
            if can_read:
                got = sock.recv(1024)

                if got.startswith(b'\00'):
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

    stop = time.time()
    print(f'finished heartbeat in %s seconds:{stop - start}')


if __name__ == '__main__':
    threads = []
    flags = {'stop': False, 'lock': threading.Lock(), 'count': 1}

    hs_address = '127.0.0.1'
    hs_port = 18901

    # ---
    # Single heartbeats testing
    # ---
    simulate_ccu_heartbeat()
    # simulate_hes_test_request(1, flags)

    # ---
    # Multiple heartbeats testing
    # ---
    # for i in range(1, no_of_heartbeats + 1):
    #     threads.append(
    #         threading.Thread(target=simulate_ccu_heartbeat, args=(i, flags))
    #     )
    #     if i % 10 == 0:
    #         threads.append(
    #             threading.Thread(
    #                 target=simulate_hes_test_request, args=(i, redis_conn, flags))
    #         )
    #
    # # Start all threads
    # for thread in threads:
    #     thread.start()
    #
    # try:
    #     # Wait for all of them to finish
    #     for thread in threads:
    #         thread.join()
    # except KeyboardInterrupt:
    #     print('\nstopping threads')
    #     flags['stop'] = True
    #     for thread in threads:
    #         thread.join()
    #
    # print('\nall done')
