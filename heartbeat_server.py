#!/usr/bin/env python3
import socket
import sys
import os
import json
import _thread
import time
import signal
import queue

__config = None
__active_threads = queue.LifoQueue()

def _load_config(filename="config.json"):
    if os.path.exists(filename):
        global __config
        with open(filename, "r") as f:
            __config = json.load(f)
        pass
    else:
        print("Failed to load configuration files")
    pass

def _client_service(conn, addr):
    print('Connected by', addr)
    with conn:
        while True:
            data = conn.recv(1024)
            if not data: break
            conn.send(data)
    print("Goodbye ", addr)
    pass

def _listener(af, socktype, proto, sa):
    s = None
    try:
        s = None
        s = socket.socket(af, socktype, proto)
        s.bind(sa)
        s.listen(100)
        while True:
            try:
                if s is None:
                    print('Could not open socket')
                else:
                    print("Start service for")
                    print(s)
                    conn, addr = s.accept()
                    __active_threads.put(_thread.start_new_thread(_client_service, (conn, addr,)))
            except OSError as err:
                print(err)
    except OSError as err:
        if s != None:
            s.close()
        print(err)
        pass

# def udp_client_service(udp_client):
#     pass

# def udp_listener():
#     pass

# def start():
#     pass

def signals_handler(signum, frame):
    print('Signal handler called with signal', signum)
    # kill all active threads
    while __active_threads.qsize() > 0:
        thr = __active_threads.get()
        print("Killing...")
        print(thr)
        thr.exit()
    # quit
    sys.exit(1)

if __name__ == "__main__":
    # print("register signal handler")
    # signal.signal(signal.SIGINT , signals_handler)

    print("load configuration")
    _load_config()

    # Start listener
    # TCP
    for res in socket.getaddrinfo(None, __config["tcp"]["port"], socket.AF_UNSPEC,
                                socket.SOCK_STREAM, 0, socket.AI_PASSIVE):
        af, socktype, proto, canonname, sa = res
        print("Start listening for")
        print((af, socktype, proto, sa,))
        __active_threads.put(_thread.start_new_thread(_listener, (af, socktype, proto, sa,)))
    # UDP
    for res in socket.getaddrinfo(None, __config["udp"]["port"], socket.AF_UNSPEC,
                                socket.SOCK_DGRAM, 0, socket.AI_PASSIVE):
        af, socktype, proto, canonname, sa = res
        print("Start listening for")
        print((af, socktype, proto, sa,))
        __active_threads.put(_thread.start_new_thread(_listener, (af, socktype, proto, sa,)))

    # all tasks will be executed in threads, go to sleep
    while True:
        time.sleep(1000)
    pass
