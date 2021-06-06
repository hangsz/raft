import os
import sys

from raft.config import config
from raft.rpc import Rpc


def create_group():
    print("create group")

    data = {
        "type": "create_group",
        "meta": {
            "num": 3,
        },
    }
    print(data)

    rpc_endpoint.send(data, (conf.ip, conf.mport))


def stop_group():    
    print("stop group") 

    data = {"type": "stop_group"}
    print(data)

    rpc_endpoint.send(data, (conf.ip, conf.mport))


def stop_master():
    print("stop master")
    data = {
        "type": "stop_master",
    }
    print(data)

    rpc_endpoint.send(data, (conf.ip, conf.mport))


if __name__ == "__main__":
    env = os.environ.get("env")
    conf = config[env] if env else config["DEV"]

    rpc_endpoint = Rpc()

    act = sys.argv[1]

    if act == "create_group":
        create_group()
    elif act == "stop_group":
        stop_group()
    elif act == "stop_master":
        stop_master()
