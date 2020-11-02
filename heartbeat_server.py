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

def _parse_heartbeat(in_data):
	# version_number_size = 2
	# source_address_size = 2
	# target_address_size = 2
	# frame_length_size = 2
	# fixed_format_size = 5
	# structure_size = 2
	# visible_string_size = 1
	# device_details_length_size = 1
	# device_details_size = value of device_details_length converted to int
	# double_long_unsigned_size = 1
	# address_size = 4
	
	try:
		version_number = in_data[0:2]
		source_address = in_data[2:4]
		target_address = in_data[4:6]
		frame_length = in_data[6:8]
		fixed_format = in_data[8:13]
		structure = in_data[13:15]
		visible_string = in_data[15:16]
		device_details_length = in_data[16:17]
		device_details_length_int = int.from_bytes(device_details_length,byteorder ='big')
		device_details = in_data[17:17+device_details_length_int]
		double_long_unsigned = in_data[17+device_details_length_int:17+device_details_length_int+1]
		address = in_data[17+device_details_length_int+1:17+device_details_length_int+5]
	
		#create out_data remember to transports the target_address and source_address
		out_fixed_format = fixed_format
		out_fixed_format_length = len(fixed_format).to_bytes(2, 'big')
		out_data = version_number+target_address+source_address+out_fixed_format_length+out_fixed_format
		
		print('version_number: ', bytearray(version_number).hex())
		print('source_address: ', bytearray(target_address).hex())
		print('target_address: ', bytearray(source_address).hex())
		print('frame_length: ', bytearray(out_fixed_format_length).hex())
		print('fixed_format: ', bytearray(out_fixed_format).hex())
		
		print('heartbeat response: ', bytearray(out_data).hex())
	except IndexError as err:
		print("badly formed heartbeat. cannot log or respond!")
		print(err)
		out_data = b'\xFF'
		
	return out_data

def _client_service(conn, addr):
    print('Connected from', addr)
    with conn:
        while True:
            data = conn.recv(1024)
            if not data: break
            conn.send(_parse_heartbeat(data))
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

    # all tasks will be executed in threads, go to sleep
    while True:
        time.sleep(1000)
    pass
