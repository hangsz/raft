#!/usr/bin/env python
# coding: utf-8

"""
@File    :   client.py
@Time    :   2022/03/19 15:35:31
@Author  :   https://github.com/hangsz
@Version :   0.1.0
@Contact :   zhenhang.sun@gmail.com
"""


import random
import sys
import time
import traceback

from raft import config, rpc


def main() -> int:
    conf = config.load_conf()

    rpc_endpoint = rpc.Endpoint((conf.IP, conf.CLIENT_PORT))

    data = {"type": "get_group"}

    rpc_endpoint.send(data, (conf.IP, conf.MASTER_PORT))
    try:
        data, _ = rpc_endpoint.recv()
        group_meta = data["meta"]

        print(group_meta)
    except Exception:
        traceback.print_exc()
        sys.exit(1)

    while True:
        try:
            res, _ = rpc_endpoint.recv(timeout=2)
            print("receive: commit success", res)
        except KeyboardInterrupt:
            rpc_endpoint.close()
            return 0
        except Exception:
            traceback.print_exc()

        addr = random.choice(group_meta["nodes"])
        data = {"type": "client_append_entries", "timestamp": int(time.time())}
        print("send: ", data)

        rpc_endpoint.send(data, addr)

        time.sleep(10)


if __name__ == "__main__":
    sys.exit(main())
