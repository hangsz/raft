import os
import sys
import json
import uuid
import random
import logging

from .config import config
from .group import Group
from .rpc import Rpc


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(funcName)s [line:%(lineno)d] %(message)s",
)
logger = logging.getLogger(__name__)


class Master(object):
    """
    Master is responsible for collect server/group's info and create group
    """

    def __init__(self):
        self.conf = self.load_conf()
        self.rpc_endpoint = Rpc((self.conf.ip, self.conf.mport))

        self.path = self.conf.master_path
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        self.groups = []
        self.server_stats = {}
        self.group_stats = {}

        self.port_used = 10000

    
    def load_conf(self):
        """
        local or remote
        """
        env = os.environ.get("env")
        conf = config[env] if env else config["DEV"]
        return conf


    def refresh_server_stats(self, data):
        pass

    def refresh_group_stats(self, data):
        pass

    def select_servers(self, num):
        """
        select servers according to the server stats
        """
        return ["localhost"] * num

    def save_group_meta(self, meta):
        filename = self.path + meta["group_id"] + ".json"
        with open(filename, "w") as f:
            json.dump(meta, f, indent=4)

    def create_group(self, meta):
        meta["group_id"] = str(uuid.uuid4())

        num = meta["num"]
        servers = self.select_servers(num)
        meta["nodes"] = [(servers[i], self.port_used + i) for i in range(num)]

        self.save_group_meta(meta)
        self.port_used += num

        group = Group(meta)

        logger.info(group.all_node_meta)
        for node_meta in group.all_node_meta:
            data = {"type": "create_node", "meta": node_meta}
            self.rpc_endpoint.send(data, (node_meta["addr"][0], self.conf.sport))

    def stop_group(self):
        filename = self.path + random.choice(os.listdir(self.path))
        with open(filename, "r") as f:
            meta = json.load(f)

        logger.info(meta)

        group = Group(meta)

        for node_meta in group.all_node_meta:
            data = {"type": "stop_node", "meta": node_meta}
            self.rpc_endpoint.send(data, (node_meta["addr"][0], self.conf.sport))

    def get_group(self, addr):
        filename = self.path + random.choice(os.listdir(self.path))
        with open(filename, "r") as f:
            meta = json.load(f)

        data = {"type": "get_group_response", "meta": meta}

        self.rpc_endpoint(data, addr)
    
    
    def stop_master(self):
        self.rpc_endpoint.close()
        sys.exit(0)

    def run(self):
        logger.info("slave begin running...")

        while True:
            try:
                data, addr = self.rpc_endpoint.recv()

                if data["type"] == "create_group":
                    logger.info("create group")

                    meta = data["meta"]
                    self.create_group(meta)

                # elif data["type"] == "create_group_node_success":
                #     logger.info(data["group_id"] + "_" + data["id"])

                # elif data["type"] == "stat":
                #     self.refresh_stat(data)

                elif data["type"] == "get_group":
                    logger.info("get group")
                    self.get_group(addr)
                    
                elif data["type"] == "stop_group":
                    logger.info("stop group")
                    self.stop_group()

                elif data["type"] == "stop_master":
                    logger.info("stop master")
                    self.stop_master()

            except Exception as e:
                logger.info(e)


if __name__ == "__main__":
    master = Master()
    master.run()
