import json
import socket


class Rpc(object):
    def __init__(self, addr=None, timeout=None):
        self.ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if addr:
            self.bind(tuple(addr))
        if timeout:
            self.ss.settimeout(timeout)

    def bind(self, addr):
        self.addr = tuple(addr)
        self.ss.bind(addr)

    def settimeout(self, timeout):
        self.ss.settimeout(timeout)

    def send(self, data, addr):
        data = json.dumps(data).encode("utf-8")
        self.ss.sendto(data, tuple(addr))

    def recv(self, addr=None, timeout=None):
        if addr:
            self.bind(addr)
        if not self.addr:
            raise ("please bind to an addr")

        if timeout:
            self.settimeout(timeout)

        data, addr = self.ss.recvfrom(65535)
        return json.loads(data), addr

    def close(self):
        self.ss.close()
