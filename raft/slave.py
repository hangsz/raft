import os
import json
import logging
from multiprocessing import Process

from .node import Node
from .config import config
from .rpc import Rpc

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(funcName)s [line:%(lineno)d]\n%(message)s')
logger = logging.getLogger(__name__)


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
        conf = config[env] if env else config['DEV']
        return conf

    def restart_raft_node(self):
        if not os.path.exists(self.path):
            return

        for file in os.listdir(self.path):
            with open(file, 'r') as f:
                node_conf = json.load(f)

            self.start_raft_node(node_conf)

    def create_node(self, meta):
        """
        start raft node
        """
        node = Node(meta)
        node.run()

    def kill_node(self, meta):
        p = self.childrens.pop((meta['group_id'], meta['id']))
        p.terminate()
        
    def save_node_meta(self, meta):
        filename = self.path + meta['group_id'] + "_" + meta['id'] + '.json'
        with open(filename, 'w') as f:
            json.dump(meta, f, indent=4)

    def run(self):
        # self.restart_raft_node()

        while True:
            data, addr = self.rpc_endpoint.recv()
            try:
                if data['type'] == "create_node":
                    logger.info("create node")

                    meta = data['meta']
                    logger.info(meta)
                    self.save_node_meta(meta)
                    p = Process(target=self.create_node, args=(meta, ), daemon=True)
                    p.start()
                    self.childrens[(meta['group_id'], meta['id'])] = p.pid

                elif  data['type'] == "kill_node":
                    logger.info("create node")
                    meta = data['meta']
                    self.stop(meta)

                elif data['type'] == "create_node_success":
                    logger.info("create node success")

            except Exception as e:
                logger.info(e)


if __name__ == "__main__":
    slave = Slave()
    slave.run()
