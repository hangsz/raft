# coding: utf-8

__author__ = 'zhenhang.sun@gmail.com'
__version__ = '1.0.0'

import os
import sys
sys.path.append("..")

from raft.rpc import Rpc
from raft.config import config


if __name__ == '__main__':
    env = os.environ.get("env")
    conf = config[env] if env else config['DEV']

    rpc_endpoint = Rpc((conf.ip, 10000))

    data = {
        "type": 'create_node',
        "meta": {
                'group_id': '1',
                'id': '0',
                'addr': ('localhost', 10001),
                'peers': {
                    '1': ('localhost', 10002),
                    '2': ('localhost', 10003)
                }
        }
    }
    rpc_endpoint.send(data, (conf.ip, conf.cport))
