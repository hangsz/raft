#!/usr/bin/env python
# coding: utf-8

"""
@File    :   config.py
@Time    :   2023/05/20 17:54:57
@Author  :   https://github.com/hangsz
@Version :   0.1.0
@Contact :   zhenhang.sun@gmail.com
"""


import os


class Config(object):
    IP = "localhost"
    MASTER_PORT = 9998  # master port
    SLAVE_PORT = 9999  # slave port
    CLIENT_PORT = 11000  # client port

    MASTER_PATH = "data/master/"
    SLAVE_PATH = "data/slave/"
    NODE_PATH = "data/node/"


class DevConfig(Config):
    env = "DEV"


class ProdConfig(Config):
    env = "PROD"


config = {"DEV": DevConfig, "PROD": ProdConfig}


def load_conf(env: str = None):
    if not env:
        env = os.getenv("RAFT_ENV", "DEV")
    conf = config[env]

    return conf
