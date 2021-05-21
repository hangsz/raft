import os
import json
import logging
from collections import namedtuple

from .config import config
from .group import Group
from .rpc import Rpc


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(funcName)s [line:%(lineno)d]\n%(message)s')
logger = logging.getLogger(__name__)

class Master(object):
    """
    Master is responsible for collect server/group's info and create group
    """

    def __init__(self):
        self.conf = self.load_conf()
        self.rpc_endpoint = Rpc((self.conf.ip, self.conf.cport))
        
        self.path = self.conf.master_path
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        self.groups = []
        self.server_stats = {}
        self.group_stats = {}

        self.port_used = 11000

    def load_conf(self):
        """
        local or remote 
        """
        env = os.environ.get("env")
        conf = config[env] if env else config['DEV']
        return conf

    def refresh_server_stats(self, data):
        pass

    def refresh_group_stats(self, data):
        pass

    def select_servers(self, num):
        """
        select servers according to the server stats
        """
        # return list(self.server_stats.keys())[:num]
        return ["localost"]*num

    def save_group_meta(self, conf):
        filename = self.path + conf['group_id'] + '.json'
        with open(filename, 'w') as f:
            json.dump(conf, f, indent=4)
        
    def create_group(self, meta):
        num = meta['num']
        servers = self.select_servers(num)
        meta['nodes'] = [(servers[i], self.port_used+i) for i in range(num)] 

        self.save_group_meta(meta)
        self.port_used += num

        group = Group(meta)
        for node_meta in group.all_node_meta:
            self.rpc_endpoint.send(node_meta, (node_meta['addr'][0], self.conf.cport))
        

    def run(self):
        while True:
            data, addr = self.rpc_endpoint.recv()

            if data['type'] == "create_group":
                logger.info("create group")

                meta = data['meta']
                self.create_group(meta)

            elif data['type'] == "create_group_node_success":
                logger.info(data['group_id']+ "_"+ data['id'])


            elif data['type'] == "stat":
                self.refresh_stat(data)

if __name__ == "__main__":
    master = Master()
    master.run()