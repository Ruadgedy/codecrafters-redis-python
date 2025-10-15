import socket  # noqa: F401
import threading

from app.resp_parser import parse_resp

BUFFER_SIZE = 2048

def handle_command(client: socket.socket):
    while chunk := client.recv(BUFFER_SIZE):
        # 客户端输入结束
        if chunk == b"":
            break
        print(f"Received client msg: {chunk.decode()}\n")

        parse = parse_resp(chunk.decode('utf-8'))
        if isinstance(parse, list): # Arrays
            if len(parse) == 1 and parse[0].lower() == "ping":
                client.send(b"+PONG\r\n")
            if len(parse) == 2 and parse[0].lower() == "echo" :
                content = parse[1]
                content_bytes = content.encode('utf-8')
                resp_str = f'${len(content_bytes)}\r\n{content}\r\n' # ECHO command
                client.sendall(resp_str.encode('utf-8'))

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, addr = server_socket.accept()  # wait for client
        print(f"Connected by: {addr}\n")
        threading.Thread(target=handle_command, args=(conn,)).start()

if __name__ == "__main__":
    main()
