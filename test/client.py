# coding: utf-8

__author__ = 'zhenhang.sun@gmail.com'
__version__ = '1.0.0'

import time 
import json
import socket
import random

from multiprocessing import Process

from raft.rpc import Rpc

def send():
    cs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    servers = [
                ('localhost', 10001),
                ('localhost', 10002), 
                ('localhost', 10003)
            ]
    
    while True:
            addr = random.choice(servers)
            
            data = {'type': 'client_append_entries', 'timestamp': int(time.time())}
            print('send: ', data)

            data = json.dumps(data).encode('utf-8')
            cs.sendto(data, addr)

            time.sleep(10)


def recv():
    addr = ('localhost', 10000)
    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ss.bind(addr)

    while True:
        data, addr = ss.recvfrom(65535)

        data = json.loads(data)
        print('recv: ' + str(data['index']) + ' has been committed')


if __name__ == '__main__':
    
    p1 = Process(target=send, name='send', daemon=True)
    p1.start()
    p2 = Process(target=recv, name='recv', daemon=True)
    p2.start()


    p1.join()
    p2.join()
