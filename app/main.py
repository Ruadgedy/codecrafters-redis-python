import socket  # noqa: F401
import threading

BUFFER_SIZE = 2048

def handle_command(client: socket.socket):
    while chunk := client.recv(BUFFER_SIZE):
        if chunk == b"":
            break
        print(f"Received client msg: {chunk.decode()}\n")
        client.send(b"+PONG\r\n")

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, addr = server_socket.accept()  # wait for client
        print(f"Connected by: {addr}\n")
        threading.Thread(target=handle_command, args=(conn,)).start()

if __name__ == "__main__":
    main()
