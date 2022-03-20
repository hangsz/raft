#!/usr/bin/env python
# coding: utf-8
'''
@File    :   slave.py
@Time    :   2022/03/19 15:04:45
@Author  :   https://github.com/hangsz
@Version :   0.1.0
@Contact :   zhenhang.sun@gmail.com
'''

import json
import logging
import os
import signal
import sys
import traceback
from multiprocessing import Process

from .config import config
from .node import Node
from .rpc import Rpc

logger = logging.getLogger(__name__)

class Slave(object):
    """
    slave is the node manager on each server
    """

    def __init__(self):
        self.conf = self.load_conf()
        self.rpc_endpoint = Rpc((self.conf.ip, self.conf.sport))
        
        self.childrens = {}
    
    @staticmethod
    def load_conf():
        env = os.environ.get("env")
        conf = config[env] if env else config["DEV"]
        return conf

    def create_node(self, meta: dict):
        """create and start a raft node

        Args:
            meta (dict): node meta
        """
        node = Node(meta)
        node.run()

    def stop_node(self, meta: dict):
        """stop a node

        Args:
            meta (dict): node meta
        """
        pid = self.childrens.pop((meta["group_id"], meta["id"]), None)
        logger.info(pid)
        if not pid:
            return
        os.kill(pid, signal.SIGTERM)

    def save_node_meta(self, meta: dict):
        """save node meta data

        Args:
            meta (dict): meta data
        """
        self.path = self.conf.slave_path
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        filename = os.path.join(self.path, meta["group_id"]+"_"+meta["id"]+".json")
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
            except KeyboardInterrupt:
                self.rpc_endpoint.close()
                sys.exit(0)
            except Exception:
                logger.info(traceback.format_exc())


def main() -> int:
    slave = Slave()
    slave.run()

    return 0

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(funcName)s [line:%(lineno)d] %(message)s",
        )
    sys.exit(main())
