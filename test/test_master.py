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
        "type": 'create_group',
        "meta": {
                'num': 3,
        }
    }
    rpc_endpoint.send(data, (conf.ip, conf.cport))
