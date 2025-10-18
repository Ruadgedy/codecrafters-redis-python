import socket  # noqa: F401
import threading
from typing import Dict, Any

from app.resp_parser import parse_resp

BUFFER_SIZE = 2048

# 存储键值对
redis_data: Dict[str, str] = {}

def to_resp(value: Any) -> bytes:
    """将python数据类型转化为RESP格式字节串"""
    if isinstance(value, str):
        # 批量字符串
        value_bytes = value.encode('utf-8')
        return f'${len(value_bytes)}\r\n{value}\r\n'.encode('utf-8')
    elif isinstance(value, int):
        # 整数
        return f':{value}\r\n'.encode('utf-8')
    elif isinstance(value, list):
        # 数组
        result = [f'*{len(value)}\r\n'.encode('utf-8')]
        for item in value:
            result.append(to_resp(item))
        return b''.join(result)
    elif isinstance(value, Exception):
        return f'-{value}\r\n'.encode('utf-8')
    elif value is None:
        return f'_\r\n'.encode('utf-8')
    elif value == True:
        # 简单字符串，用于PING应答
        return f'+PONG\r\n'.encode('utf-8')
    else:
        return to_resp(str(value))

def parse_resp(data: bytes) -> Any:
    """将RESP字节串转化为Python类型"""
    if not data:
        return None

    pos = 0
    type_char = data[pos:pos+1]
    pos += 1

    # 解析数组，redis命令会按照数组形式传递
    if type_char == b'*':
        end = data.find(b'\r\n')
        if end == -1:
            raise ValueError("Invalid RESP format,missing CRLF")
        count = int(data[pos:end])
        pos += 3

        result = []
        for i in range(count):
            # 依次解析每个元素
            if pos >= len(data) or data[pos:pos+1] != b'$':
                raise ValueError("Invalid RESP format, expected bulk string")
            pos += 1

            # 读取字符串长度
            end = data.find(b'\r\n',pos)
            if end == -1:
                raise ValueError("Invalid RESP format, missing CRLF")
            length = int(data[pos:end])
            pos += 3

            # 读取字符串
            if pos + length + 2 > len(data):
                raise ValueError("Invalid RESP format, insufficient data")
            value = data[pos:pos+length].decode('utf-8')
            result.append(value)
            pos = pos + length + 2

        return result
    raise ValueError(f"Unsupported RESP format: {type_char} ")


def handle_command(client: socket.socket):
    # 客户端处理请求
    try:
        while True:
            data = client.recv(BUFFER_SIZE)
            if not data:
                break

            try:
                # 解析RESP命令
                command = parse_resp(data)
                if not command:
                    continue

                # 转换命令为大写
                cmd = command[0].upper()
                response: Any = None

                # 处理不同命令
                if cmd == 'PING':
                    response = True
                elif cmd == 'ECHO':
                    if  len(command) > 1:
                        response = command[1]
                    else:
                        response = Exception("ERR wrong number of arguments for 'echo' command")
                elif cmd == 'SET':
                    if len(command) >= 3:
                        redis_data[command[1]] = command[2]
                        response = "OK"
                    else:
                        response = Exception("ERR wrong number of arguments for 'set' command")
                elif cmd == 'GET':
                    if len(command) >= 2:
                        response = redis_data.get(command[1],None)
                    else:
                        response = Exception("ERR wrong number of arguments for 'get' command")
                else:
                    response = Exception(f"ERR unknown command '{cmd}'")

                # 发送RESP格式答案
                client.send(to_resp(response))
            except Exception as e:
                # 发送错误响应
                client.sendall(to_resp(Exception(f"ERR {str(e)}")))
    except ConnectionResetError:
        print("Client disconnected abruptly")
    finally:
        print("Client disconnected")
        client.close()


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, addr = server_socket.accept()  # wait for client
        print(f"Connected by: {addr}\n")
        threading.Thread(target=handle_command, args=(conn,)).start()

if __name__ == "__main__":
    main()
