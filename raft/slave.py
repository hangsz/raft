#!/usr/bin/env python
# coding: utf-8
"""
@File    :   slave.py
@Time    :   2022/03/19 15:04:45
@Author  :   https://github.com/hangsz
@Version :   0.1.0
@Contact :   zhenhang.sun@gmail.com
"""

import json
import logging
import os
import signal
import sys
import traceback
from multiprocessing import Process

from . import config, rpc
from .node import Node

logger = logging.getLogger(__name__)


class Slave(object):
    """
    slave is the node manager on each server
    """

    def __init__(self):
        self.conf = config.load_conf()
        self.rpc_endpoint = rpc.Endpoint((self.conf.IP, self.conf.SLAVE_PORT))

        self.childrens = {}

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
        self.path = self.conf.SLAVE_PATH
        os.makedirs(self.path, exist_ok=True)

        filename = os.path.join(
            self.path, meta["group_id"] + "_" + meta["id"] + ".json"
        )
        with open(filename, "w") as f:
            json.dump(meta, f, indent=4)

    def stop_slave(self):
        self.rpc_endpoint.close()
        sys.exit(0)

    def run(self):
        while True:
            logger.info("slave is listening...")
            try:
                data, _ = self.rpc_endpoint.recv()
                logger.info(f"{data['type']}  start...")

                if data["type"] == "create_node":
                    meta = data["meta"]
                    logger.info(f"meta: {meta}")
                    self.save_node_meta(meta)
                    p = Process(target=self.create_node, args=(meta,), daemon=True)
                    p.start()
                    self.childrens[(meta["group_id"], meta["id"])] = p.pid
                elif data["type"] == "stop_node":
                    meta = data["meta"]
                    logger.info(f"meta: {meta}")
                    self.stop_node(meta)
                elif data["type"] == "stop_slave":
                    self.stop_slave()
            except KeyboardInterrupt:
                self.rpc_endpoint.close()
                sys.exit(0)
            except Exception:
                logger.info(traceback.format_exc())
            finally:
                logger.info(f"{data['type']}  end.")


def main() -> int:
    slave = Slave()
    slave.run()

    return 0


if __name__ == "__main__":
    sys.exit(main())
