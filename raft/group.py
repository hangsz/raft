class Group(object):
    """
    raft group: one group has 3 or 5 or more raft nodes
    """

    def __init__(self, meta):
        self.range = (0, 100)
        self.meta = meta

    @property
    def all_node_meta(self):
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
