#!/usr/bin/env python
# coding: utf-8
'''
@File    :   group.py
@Time    :   2022/03/19 14:45:02
@Author  :   https://github.com/hangsz
@Version :   0.1.0
@Contact :   zhenhang.sun@gmail.com
'''

class Group(object):
    """
    raft group: one group has 3 or 5 or more raft nodes
    """

    def __init__(self, meta: dict):
        """

        Args:
            meta (dict): group meta data
        """
        self.range = (0, 100)
        self.meta = meta

    @property
    def all_node_meta(self)-> list[dict]:
        """return all nodes meta data in list

        Returns:
            list[dict]: meta data of all nodes
        """
        res = []
        num = self.meta["num"]
        for i in range(num):
            node_meta = {
                "group_id": self.meta["group_id"],
                "id": str(i),
                "addr": self.meta["nodes"][i],
                "peers": {j: self.meta["nodes"][j] for j in range(num) if j != i},
            }

            res.append(node_meta)

        return res

    def add_node(self):
        pass

    def delete_node(self):
        pass

    def change_leader(self):
        pass
