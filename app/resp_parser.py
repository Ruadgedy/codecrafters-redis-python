import socket


def parse_resp(data: str):

    if isinstance(data, str):
        data.encode('utf-8')

    # 使用生成器追踪解析位置
    class Parser:
        def __init__(self, data):
            self.data = data
            self.pos = 0

        def read_line(self):
            # 读取一行，不包括最后的\r\n
            start = self.pos
            while self.pos < len(self.data):
                if self.data[self.pos:self.pos+2] == b'\r\n':
                    line = self.data[start:self.pos]
                    self.pos += 2
                    return line.decode('utf-8')
                self.pos += 1
            raise ValueError("不完整的RESP结构，缺少\r\n")

        def parse(self):
            if self.pos >= len(self.data):
                return None

            # 查看第一个字符确定类型
            type_char = self.data[self.pos]
            self.pos += 1
            if type_char == b'*':   # Arrays
                array_len = int(self.read_line())
                if array_len == -1: # Empty array
                    return None
                array = []
                for i in range(array_len):
                    array.append(self.parse())
                return array
            elif type_char == b'+': # Simple string
                return self.read_line()
            elif type_char == b':': # Integer
                return int(self.read_line())
            elif type_char == b'$': # Bulk string
                str_len = int(self.read_line())
                self.pos += 2
                return self.read_line()
            elif type_char == b'-': # Error
                raise ValueError(self.read_line())
            else:
                raise ValueError(f"Unknow RESP type: {type_char}")

    parser = Parser(data)
    return parser.parse()