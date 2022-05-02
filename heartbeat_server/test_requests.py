
import random
import select
import string
import socket
import time
import threading

import redis


heartbeat = b'\x00\x01\x00\x01\x00\x66\x00\x1E\x0F\xC0\x00\x00\x00\x02\x02\x0A\x10\x4D\x54\x52\x4B\x30\x31\x37\x39\x30\x30\x30\x31\x33\x32\x30\x33\x06\x00\x00\x00\x00'
test_request = b'{"key": "%s", "meter": "179000222382", "PA": "3", "PASSWORD": "11111111", "RANDOM": 31}|\x01R1\x020.9.2.255()\x03D'

redis_queue = 'test_request_queue'
redis_url = 'redis://veros:bb5qFU9xFMPCWpEJoKOe60zSN1e6LOkT@redis-10661.c259.us-central1-2.gce.cloud.redislabs.com:10661'
# redis_url = 'redis://127.0.0.1:6379'

address = '54.197.44.252'
# address = '127.0.0.1'
port = 18901

no_of_heartbeats = 1000


def generate_key():
    return ''.join(
        [
            random.choice(string.ascii_letters+string.digits) \
                for _ in range(20)
        ]
    )

def send_heartbeat(idx, flags):
    start = time.time()
    print('starting heartbeat no.%s ' % idx)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((address, port))
    sock.send(heartbeat)

    while not flags['stop']:
        try:
            can_read, _, error = select.select([sock,], [], [], 5)
            if can_read:
                got = sock.recv(1024)
                if not got:
                    break
            if error:
                raise select.error(str(error))
        except select.error as e:
            sock.shutdown(2)
            sock.close()
            break

    stop = time.time()
    flags['lock'].acquire()
    count = flags['count']
    flags['count'] += 1
    flags['lock'].release()
    print('(%s) finished heartbeat no.%s in %s seconds: ' % (count, idx, stop-start))


def send_test_request(idx, redis_conn, flags):
    start = time.time()
    print('starting test_request, thread no.%s ' % idx)

    key = generate_key()
    key_bytes = bytes(key, encoding='utf-8')
    redis_conn.lpush(redis_queue, test_request % key_bytes)
    response = None

    while not flags['stop']:
        try:
            response = redis_conn.blpop(redis_queue, 1)
            if response is None:
                continue
        except Exception as e:
            break

    stop = time.time()
    print('response: %r' % (response,))

    flags['lock'].acquire()
    count = flags['count']
    flags['lock'].release()

    print('(%s) finished test_request, thread no.%s in %s seconds: ' % (count, idx, stop-start))



if __name__ == '__main__':
    threads = []
    flags = {'stop': False, 'lock': threading.Lock(), 'count': 1}

    redis_conn = redis.StrictRedis.from_url(redis_url)

    for i in range(1, no_of_heartbeats+1):
        threads.append(
            threading.Thread(target=send_heartbeat, args=(i, flags))
        )
        if i % 10 == 0:
            threads.append(
                threading.Thread(
                    target=send_test_request, args=(i, redis_conn, flags))
            )

    # Start all threads
    for thread in threads:
        thread.start()

    try:
        # Wait for all of them to finish
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print('\nstopping threads')
        flags['stop'] = True
        for thread in threads:
            thread.join()

    print('\nall done')
