import os
import sys

from raft.config import config
from raft.rpc import Rpc


def create_node():
    metas = [
        {
            "meta": {
                "group_id": "2",
                "id": "0",
                "addr": ("localhost", 10000),
                "peers": {"1": ("localhost", 10001), "2": ("localhost", 10002)},
            },
        },
        {
            "meta": {
                "group_id": "2",
                "id": "1",
                "addr": ("localhost", 10001),
                "peers": {"0": ("localhost", 10000), "2": ("localhost", 10002)},
            },
        },
        {
            "meta": {
                "group_id": "2",
                "id": "2",
                "addr": ("localhost", 10002),
                "peers": {"0": ("localhost", 10000), "1": ("localhost", 10001)},
            },
        },
    ]

    # create node
    for meta in metas:
        data = {
            'type': 'create_node',
            'meta': meta
        }
        rpc_endpoint.send(data, (conf.ip, conf.cport))


    rpc_endpoint.close()


def stop_slave():
    data = {'type': "stop_slave"}

    rpc_endpoint.send(data, (conf.ip, conf.sport))


if __name__ == "__main__":
    env = os.environ.get("env")
    conf = config[env] if env else config["DEV"]

    rpc_endpoint = Rpc()

    act = sys.argv[1]

    if act == "create_node":
        create_node()
    elif act == "stop_slave":
        stop_slave()
