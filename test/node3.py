# coding: utf-8

__author__ = 'zhenhang.sun@gmail.com'
__version__ = '1.0.0'

import sys
sys.path.append("..")

from raft.node import Node

if __name__ == '__main__':
    
    # 修改成
    conf = {  'group_id': '1',
              'id': '3',
              'addr': ('localhost', 10003),
              'peers': { '1': ('localhost', 10001), 
                         '2': ('localhost', 10002)
                       }
            }
     
    node = Node(conf)

    node.run()