import socket  # noqa: F401
import threading
import time
from typing import Dict, Any, Optional, List

from app.resp_parser import parse_resp

BUFFER_SIZE = 2048

# 存储键值对 {key: ("string", value, expire | ("list", [elements])}
'''
tuple[str, Any, Optional[float]]字典的值（value） 必须是一个三元组（tuple），且三元组的三个元素类型固定：
第一个元素：str
表示某种 “类型标识”（通常用于区分值的业务类型），例如 "string"、"list" 等。
第二个元素：Any
表示具体存储的值，类型不受限制（可以是字符串、列表、数字等任意类型）。
第三个元素：Optional[float]
表示过期时间戳，类型可以是 float（浮点数时间戳，如 1620000000.5）或 None（表示永不过期）。
'''
redis_data: Dict[str, tuple[str,Any, Optional[float]]] = {}

def to_resp(value: Any) -> bytes:
    """将python数据类型转化为RESP格式字节串"""
    if isinstance(value, str):
        # 批量字符串
        value_bytes = value.encode('utf-8')
        return f'${len(value_bytes)}\r\n{value}\r\n'.encode('utf-8')
    elif value is True:
        # 简单字符串，用于PING应答
        return f'+PONG\r\n'.encode('utf-8')
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
        return f'$-1\r\n'.encode('utf-8')
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
            pos = end + 2

            # 读取字符串
            if pos + length + 2 > len(data):
                raise ValueError("Invalid RESP format, insufficient data")
            value = data[pos:pos+length].decode('utf-8')
            result.append(value)
            pos = pos + length + 2

        return result
    raise ValueError(f"Unsupported RESP format: {type_char} ")


def is_expire(expire: Optional[float]) -> bool:
    if expire is None:
        return False
    return time.time() > expire


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
                    if len(command) < 3:
                        response = Exception("ERR wrong number of arguments for 'set' command")
                    else:
                        key = command[1]
                        value = command[2]
                        expire: Optional[float] = None # 过期时间戳

                        i = 3 # PX or EX if exists
                        while i < len(command):
                            param = command[i].upper()  # PX or EX
                            if param in ('EX', 'PX') and i+1 < len(command):
                                try:
                                    ttl = int(command[i + 1])
                                    if ttl <= 0:
                                        raise ValueError("invalid expire time in 'set' command")
                                except ValueError:
                                    response = Exception("ERR expire time. Is not int")
                                    break

                                if param == 'EX':
                                    expire = time.time() + ttl
                                else:
                                    expire = time.time() + ttl/1000
                                i += 2
                            else:
                                response = Exception(f"ERR syntax error")
                                break
                        else:
                            redis_data[key] = ("string",value, expire)
                            response = "OK"

                elif cmd == 'GET':
                    if len(command) < 2:
                        response = Exception("ERR wrong number of arguments for 'get' command")
                    else:
                        key = command[1]
                        if key in redis_data.keys():
                            # Inspect key if exists and not expire
                            type_, value, expire = redis_data[key]
                            if type_ != "string":
                                response = None
                            if is_expire(expire):
                                # delete expired key
                                del redis_data[key]
                                response = None
                            else:
                                response = value
                        else:
                            response = None # key not exists
                elif cmd in ("LPUSH", "RPUSH"):
                    # command format
                    # RPUSH list_name element1 [element2...]
                    if len(command) < 3:
                        response = Exception(f"ERR wrong number of arguments for '{cmd}' command")
                    else:
                        key = command[1]
                        elements = command[2:]

                        # check key if exists
                        if key in redis_data.keys():
                            type_, value, expire = redis_data[key]
                            if type_ != "list":
                                response = Exception(f"WRONGTYPE Operation against a key holding the wrong kind of value")
                                client.send(to_resp(response))
                                continue
                            lst = value
                        else:   # key not exists
                            lst: List[str] = []

                        # Add elements
                        if cmd == "LPUSH":
                            lst = elements + lst
                        else:
                            lst = lst + elements
                        redis_data[key] = ("list", lst, None)

                        # return List length
                        response = len(lst)
                elif cmd == "LRANGE":
                    # command format: LRANGE key start stop
                    '''
                    合法性检查：
                    1、列表不存在，返回空列表
                    2、如果start大于列表长度，返回空列表
                    3、如果stop大于等于列表长度，stop指向最后一次元素
                    4、如果 start>stop，返回空列表
                    '''
                    if len(command) != 4:
                        response = Exception(f"ERR wrong number of arguments for '{cmd}' command")
                    else:
                        key = command[1]
                        if key in redis_data.keys():
                            start = int(command[2])
                            stop = int(command[3])
                            type_, value, _= redis_data[key]
                            if type_ != "list":
                                response = None
                            else:
                                list_len = len(value)
                                if start > list_len or start > stop:
                                    response = []
                                else:
                                    if stop >= list_len:
                                        stop = list_len-1
                                    response = value[start:stop+1]
                        else:
                            response = []

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
