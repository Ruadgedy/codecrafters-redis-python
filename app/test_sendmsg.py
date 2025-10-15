import socket
import unittest

class TestSendmsg(unittest.TestCase):
    def test_sendmsg(self):
        client_conn = socket.create_connection(("localhost", 6379))
        client_conn.send(b"Hello")
        msg = client_conn.recv(1024)
        print(str(msg))
        client_conn.close()