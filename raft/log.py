import os
import json


class Log(object):
    def __init__(self, filename):
        self.filename = filename

        if os.path.exists(self.filename):
            with open(self.filename, "r") as f:
                self.entries = json.load(f)
        else:
            self.entries = []

    @property
    def last_log_index(self):
        return len(self.entries) - 1

    @property
    def last_log_term(self):
        return self.get_log_term(self.last_log_index)

    def get_log_term(self, log_index):
        """
        leader do
        follower
        """
        if log_index >= len(self.entries):
            return -1
        elif log_index < 0:
            return -1
        else:
            return self.entries[log_index]["term"]

    def get_entries(self, next_index):
        """
        leader do
        """
        # print('get_entries')
        return self.entries[max(0, next_index):]

    def delete_entries(self, prev_log_index):
        # print('delete_entries')

        self.entries = self.entries[: max(0, prev_log_index)]
        self.save()

    def append_entries(self, prev_log_index, entries):
        # print('append_entries')

        self.entries = self.entries[: max(0, prev_log_index + 1)] + entries

        self.save()

    def save(self):
        with open(self.filename, "w") as f:
            json.dump(self.entries, f, indent=4)
