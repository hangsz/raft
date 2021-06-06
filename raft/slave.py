import os, signal
import sys
import json
import logging
from multiprocessing import Process

from .node import Node
from .config import config
from .rpc import Rpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(funcName)s [line:%(lineno)d] %(message)s",
)
logger = logging.getLogger(__name__)


def exit():
    logger.info("exit")
    sys.exit(0)


class Slave(object):
    """
    slave is the node manager on each server
    """

    def __init__(self):
        self.conf = self.load_conf()
        self.rpc_endpoint = Rpc((self.conf.ip, self.conf.sport))

        self.path = self.conf.slave_path
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        self.childrens = {}

    def load_conf(self):
        env = os.environ.get("env")
        conf = config[env] if env else config["DEV"]
        return conf

    def restart_raft_node(self):
        if not os.path.exists(self.path):
            return

        for file in os.listdir(self.path):
            with open(file, "r") as f:
                node_conf = json.load(f)

            self.start_raft_node(node_conf)


    def create_node(self, meta):
        """
        start raft node
        """
        node = Node(meta)
        node.run()

    def stop_node(self, meta):
        pid = self.childrens.pop((meta["group_id"], meta["id"]), None)
        logger.info(pid)
        if not pid:
            return 
        os.kill(pid, signal.SIGTERM)

    def save_node_meta(self, meta):
        filename = self.path + meta["group_id"] + "_" + meta["id"] + ".json"
        with open(filename, "w") as f:
            json.dump(meta, f, indent=4)

    def stop_slave(self):
        self.rpc_endpoint.close()
        sys.exit(0)

    def run(self):
        logger.info("slave begin running...")

        while True:
            try:
                data, addr = self.rpc_endpoint.recv()

                if data["type"] == "create_node":
                    logger.info("create node")

                    meta = data["meta"]
                    logger.info(meta)
                    self.save_node_meta(meta)
                    p = Process(target=self.create_node, args=(meta,), daemon=True)
                    p.start()
                    self.childrens[(meta["group_id"], meta["id"])] = p.pid

                elif data["type"] == "stop_node":
                    logger.info("stop node")
                    meta = data["meta"]
                    self.stop_node(meta)

                elif data["type"] == "stop_slave":
                    logger.info("stop slave")
                    self.stop_slave()

            except Exception as e:
                logger.info(e)


if __name__ == "__main__":
    slave = Slave()
    slave.run()
