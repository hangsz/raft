# coding: utf-8

__author__ = 'zhenhang.sun@gmail.com'
__version__ = '1.0.0'

from node import Node

if __name__ == '__main__':
    
    # 修改成
    conf = {'id': 'node_3',
              'addr': ('localhost', 10003),
              'peers': { 'node_1': ('localhost', 10001), 
                         'node_2': ('localhost', 10002)
                       }
            }
     
    node = Node(conf)

    node.run()