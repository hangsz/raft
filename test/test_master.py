#!/usr/bin/env python
# coding: utf-8
'''
@File    :   test_master.py
@Time    :   2022/03/19 15:36:46
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

    if act == "create_group":
        data = {"type": "create_group",
                "meta": {"num": 3}}
    elif act == "stop_group":
        data = {"type": "stop_group"}
    elif act == "stop_master":
        data = {"type": "stop_master"}

    print(data)

    rpc_endpoint.send(data, (conf.ip, conf.mport))
    rpc_endpoint.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
