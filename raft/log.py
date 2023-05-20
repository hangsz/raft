#!/usr/bin/env python
# coding: utf-8

"""
@File    :   log.py
@Time    :   2023/05/20 18:11:55
@Author  :   https://github.com/hangsz
@Version :   0.1.0
@Contact :   zhenhang.sun@gmail.com
"""


import os
import json


class Log(object):
    def __init__(self, filename: str):
        """_summary_

        Args:
            filename (str): .log path
        """
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

    def get_log_term(self, log_index: int) -> int:
        """

        Args:
            log_index (int):

        Returns:
            int:
        """

        if log_index < 0 or log_index >= len(self.entries):
            return -1
        else:
            return self.entries[log_index]["term"]

    def get_entries(self, next_index: int) -> list[dict]:
        """get entries

        Args:
            next_index (int): next index

        Returns:
            list[dict]:
        """
        return self.entries[max(0, next_index) :]

    def delete_entries(self, prev_log_index: int):
        """delete entries

        Args:
            prev_log_index (int): log index
        """
        if prev_log_index < 0 or prev_log_index >= len(self.entries):
            return
        self.entries = self.entries[: max(0, prev_log_index)]
        self.save()

    def append_entries(self, prev_log_index: int, entries: list[dict]):
        """

        Args:
            prev_log_index (int): log index
            entries (list[dict]):
        """
        self.entries = self.entries[: max(0, prev_log_index + 1)] + entries
        self.save()

    def save(self):
        with open(self.filename, "w") as f:
            json.dump(self.entries, f, indent=4)
