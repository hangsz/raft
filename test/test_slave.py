#!/usr/bin/env python
# coding: utf-8
'''
@File    :   test_slave.py
@Time    :   2022/03/19 21:56:43
@Author  :   https://github.com/hangsz
@Version :   0.1.0
@Contact :   zhenhang.sun@gmail.com
'''

import os
import sys

from raft.config import config
from raft.rpc import Rpc


def main() -> int:
    env = os.environ.get("env")
    conf = config[env] if env else config["DEV"]

    rpc_endpoint = Rpc()

    act = sys.argv[1]

    if act == "create_node":
        metas = [{"meta": {"group_id": "2",
                           "id": "0",
                           "addr": ("localhost", 10000),
                           "peers": {"1": ("localhost", 10001), "2": ("localhost", 10002)}}},
                 {"meta": {"group_id": "2",
                           "id": "1",
                           "addr": ("localhost", 10001),
                           "peers": {"0": ("localhost", 10000), "2": ("localhost", 10002)}}},
                 {"meta": {"group_id": "2",
                           "id": "2",
                           "addr": ("localhost", 10002),
                           "peers": {"0": ("localhost", 10000), "1": ("localhost", 10001)}}}
                 ]

        for meta in metas:
            data = {
                'type': 'create_node',
                'meta': meta
            }
            print(data)
            rpc_endpoint.send(data, (conf.ip, conf.cport))

    elif act == "stop_slave":
        data = {'type': "stop_slave"}
        print(data)
        rpc_endpoint.send(data, (conf.ip, conf.sport))
    
    rpc_endpoint.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
