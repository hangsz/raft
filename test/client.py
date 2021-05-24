import os
import time
import json
import socket
import random
import logging

import sys

sys.path.append("..")

from multiprocessing import Process
from raft.config import config
from raft.rpc import Rpc


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(funcName)s [line:%(lineno)d]\n%(message)s",
)
logger = logging.getLogger(__name__)


def send(meta):

    rpc_endpoint = Rpc()

    while True:
        addr = random.choice(meta["nodes"])

        data = {"type": "client_append_entries", "timestamp": int(time.time())}
        print("send: ", data)

        data = json.dumps(data).encode("utf-8")
        rpc_endpoint.send(data, addr)

        time.sleep(10)


def recv():
    addr = ("localhost", 10000)
    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ss.bind(addr)

    while True:
        data, addr = ss.recvfrom(65535)

        data = json.loads(data)
        print("recv: " + str(data["index"]) + " has been committed")


if __name__ == "__main__":
    env = os.environ.get("env")
    conf = config[env] if env else config["DEV"]

    rpc_endpoint = Rpc((conf.ip, conf.cport))

    data = {"type": "get_group"}

    rpc_endpoint.send(data, (conf.ip, conf.mport))
    group_meta, _ = rpc_endpoint.recv()
    print(group_meta)

    while True:
        try:
            res, _ = rpc_endpoint.recv(timeout=2)
            print("recv: commit success", res)
        except Exception as e:
            pass
        addr = random.choice(group_meta["nodes"])
        data = {"type": "client_append_entries", "timestamp": int(time.time())}
        print("send: ", data)

        rpc_endpoint.send(data, addr)

        time.sleep(10)
