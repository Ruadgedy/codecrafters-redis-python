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
        b = conn.recv(2048)
        st = b.decode().strip().split("\n")
        for s in st:
            print(s)
            if s == "PING":
                print("sending pong")
                conn.sendall(b"+PONG\r\n")
        # conn.sendall(b"+Chat End\r\n")
    conn.close()

if __name__ == "__main__":
    main()
