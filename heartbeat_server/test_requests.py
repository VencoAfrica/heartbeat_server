import asyncio
import random
import select
import string
import socket
import time
import threading

from iec62056_21.messages import CommandMessage


redis_queue = 'test_request_queue'
# redis_url = 'redis://veros:bb5qFU9xFMPCWpEJoKOe60zSN1e6LOkT@redis-10661.c259.us-central1-2.gce.cloud.redislabs.com:10661'
redis_url = 'redis://127.0.0.1:6379'

no_of_heartbeats = 1000


def simulate_ccu_heartbeat():
    start = time.time()
    print('starting heartbeat')

    heartbeat = b'\x00\x01\x00\x01\x00\x66\x00\x1E\x0F\xC0\x00\x00\x00\x02\x02\x0A\x10\x4D\x54\x52\x4B\x30\x31\x37\x39\x30\x30\x30\x31\x33\x32\x30\x33\x06\x00\x00\x00\x00'

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((hs_address, hs_port))
    sock.send(heartbeat)

    while True:
        try:
            can_read, _, error = select.select([sock, ], [], [], 5)
            if can_read:
                got = sock.recv(1024)
                print(f'got %s', got)
            # if not can_read:
            #     break
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


async def test_reads(reader, writer, meter, logger=None, codes=None):
    codes = codes or [
        ('voltage', '32.7.0.255'), ('time', '0.9.1.255'),
        ('date', '0.9.2.255'),
    ]
    for label, code in codes:
        msg = CommandMessage.for_single_read(code).to_bytes()
        PA, password = DEFAULT_PASSWORD_LEVEL, DEFAULT_PASSWORD
        to_send = prep_data(
            meter, PA, DEFAULT_RANDOM_NUMBER, password, msg
        )
        if logger:
            logger.info(
                "Sending Data [%s %r]: %s", label, (PA, password), to_send.hex())
        try:
            response = await send_data(to_send, reader, writer, logger)
            if logger:
                logger.info(
                    "write response [%s %r]: %s", label, (PA, password), response.hex())
        except BrokenPipeError:
            writer.close()
            if logger:
                logger.exception("broken pipe on time read")
