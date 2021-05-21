# coding: utf-8

__author__ = 'zhenhang.sun@gmail.com'
__version__ = '1.0.0'

import sys
sys.path.append("..")

from raft.node import Node

if __name__ == '__main__':

    meta = {   
              'group_id': '1',
              'id': '0',
              'addr': ('localhost', 10001),
              'peers': { '1': ('localhost', 10002), 
                         '2': ('localhost', 10003)
                       }
    }

    node = Node(meta)

    node.run()