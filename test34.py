import socket
import datetime

def heartbeat_logger_server2(host, port, log_file):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        # Bind the server socket to the specified host and port
        server_socket.bind((host, port))
        print(f"Server listening on {host}:{port}")

        # Listen for incoming connections (only one connection at a time for simplicity)
        server_socket.listen(1)

        while True:
            # Accept a connection
            client_socket, client_address = server_socket.accept()
            print(f"Connection established from {client_address}")

            try:
                # Receive binary data (assuming heartbeats are sent as bytes)
                data = client_socket.recv(1024)

                # Log the heartbeat to the file
                with open(log_file, 'ab') as log:  # Use 'ab' for binary append mode
                    timestamp = datetime.datetime.now().isoformat()
                    log.write(f"{timestamp} - Received heartbeat: {data}\n".encode('utf-8'))

            except Exception as e:
                print(f"Error processing heartbeat: {str(e)}")
            
            finally:
                # Close the client socket
                client_socket.close()

# Define the host, port, and log file
host = '0.0.0.0'  # Listens on all available interfaces
port = 15870  # Choose a port number (change if needed)
log_file = 'heartbeat_log.txt'  # Specify the log file name

# Start the heartbeat logger server
heartbeat_logger_server2(host, port, log_file)