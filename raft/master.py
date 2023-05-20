#!/usr/bin/env python
# coding: utf-8
"""
@File    :   master.py
@Time    :   2022/03/19 14:46:06
@Author  :   https://github.com/hangsz
@Version :   0.1.0
@Contact :   zhenhang.sun@gmail.com
"""

import json
import logging
import os
import random
import sys
import traceback
import uuid

from . import config, rpc
from .group import Group

logger = logging.getLogger(__name__)


class Master(object):
    """
    Master is responsible for collect server/group's info and create group
    """

    def __init__(self):
        self.conf = config.load_conf()
        self.rpc_endpoint = rpc.Endpoint((self.conf.IP, self.conf.MASTER_PORT))

        self.groups = []
        self.server_stats = {}
        self.group_stats = {}

        self.port_used = 10000

    def refresh_server_stats(self, data):
        pass

    def refresh_group_stats(self, data):
        pass

    @staticmethod
    def select_servers(num: int) -> list[str]:
        """select servers according to the server stats

        Args:
            num (int): number of servers

        Returns:
            list[str]: a list of ip addr
        """
        return ["localhost"] * num

    def save_group_meta(self, meta: dict):
        """save group meta data

        Args:
            meta (dict): group meta data
        """

        self.path = self.conf.MASTER_PATH
        os.makedirs(self.path, exist_ok=True)

        filename = os.path.join(self.path, meta["group_id"] + ".json")
        with open(filename, "w") as f:
            json.dump(meta, f, indent=4)

    def create_group(self, meta: dict):
        """create a new group

        Args:
            meta (dict): group meta data
        """
        meta["group_id"] = str(uuid.uuid4())

        num = meta["num"]
        servers = self.select_servers(num)
        meta["nodes"] = [(servers[i], self.port_used + i) for i in range(num)]

        self.save_group_meta(meta)
        self.port_used += num

        group = Group(meta)
        for node_meta in group.all_node_meta:
            data = {"type": "create_node", "meta": node_meta}
            logger.info(f"request slave to create node: {data}")
            self.rpc_endpoint.send(data, (node_meta["addr"][0], self.conf.SLAVE_PORT))

    def stop_group(self):
        filename = os.path.join(self.path, random.choice(os.listdir(self.path)))
        with open(filename, "r") as f:
            meta = json.load(f)

        logger.info(meta)
        group = Group(meta)

        for node_meta in group.all_node_meta:
            data = {"type": "stop_node", "meta": node_meta}
            logger.info(data)
            self.rpc_endpoint.send(data, (node_meta["addr"][0], self.conf.SLAVE_PORT))

    def get_group(self, addr: tuple[str, int]):
        """get a group meta and send

        Args:
            addr (tuple[str, int]): ip and port
        """
        filename = os.path.join(self.path, random.choice(os.listdir(self.path)))
        with open(filename, "r") as f:
            meta = json.load(f)
        data = {"type": "get_group_response", "meta": meta}
        self.rpc_endpoint.send(data, addr)

    def stop_master(self):
        self.rpc_endpoint.close()
        sys.exit(0)

    def run(self):
        while True:
            logger.info("master is listening...")
            try:
                data, addr = self.rpc_endpoint.recv()
                logger.info(f"{data['type']}  start...")

                if data["type"] == "create_group":
                    meta = data["meta"]
                    logger.info(f"meta: {meta}")
                    self.create_group(meta)
                elif data["type"] == "get_group":
                    self.get_group(addr)
                elif data["type"] == "stop_group":
                    self.stop_group()
                elif data["type"] == "stop_master":
                    self.stop_master()
            except KeyboardInterrupt:
                self.rpc_endpoint.close()
                sys.exit(0)
            except Exception:
                logger.info(traceback.format_exc())

            finally:
                logger.info(f"{data['type']} end.")


def main() -> int:
    master = Master()
    master.run()

    return 0


if __name__ == "__main__":
    sys.exit(main())
