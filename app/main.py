import socket  # noqa: F401

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn,addr = server_socket.accept() # wait for client
    print(f"Connected to server! Address:{addr}")

    while True:
        msg = conn.recv(1024).decode()
        # print(msg)
        # 客户端读取的输入不只是“PING”，而是“*1\r\n$4\r\nPING\r\n“，要考虑处理RESP
        msg = msg.split("\n")
        for s in msg:
            print(str(s))
            if s == "PING":
                conn.sendall(b"+PONG\r\n")
    conn.close()

if __name__ == "__main__":
    main()
