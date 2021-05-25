import json
import logging
import os
import random
import time

from raft.config import config
from raft.rpc import Rpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(funcName)s [line:%(lineno)d]\n%(message)s",
)
logger = logging.getLogger(__name__)



if __name__ == "__main__":
    env = os.environ.get("env")
    conf = config[env] if env else config["DEV"]

    rpc_endpoint = Rpc((conf.ip, conf.cport))

    data = {"type": "get_group"}

    rpc_endpoint.send(data, (conf.ip, conf.mport))
    try:
        group_meta, _ = rpc_endpoint.recv(timeout=2)
        print(group_meta)
    except Exception as e:
        print(e)

    group_meta = {"nodes": [("localhost", 10001)]}

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
