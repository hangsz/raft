import os

from raft.config import config
from raft.rpc import Rpc

if __name__ == "__main__":

    env = os.environ.get("env")
    conf = config[env] if env else config["DEV"]

    rpc_endpoint = Rpc()

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
    # for meta in metas:
    #     data = {
    #         'type': 'create_node',
    #         'meta': meta
    #     }
    #     rpc_endpoint.send(data, (conf.ip, conf.cport))

    # kill node
    for meta in metas:
        data = {"type": "kill_node", "meta": meta}
        rpc_endpoint.send(data, (conf.ip, conf.cport))
