#!/usr/bin/env python
# coding: utf-8
'''
@File    :   config.py
@Time    :   2022/03/19 14:45:22
@Author  :   https://github.com/hangsz
@Version :   0.1.0
@Contact :   zhenhang.sun@gmail.com
'''


class Config(object):
    ip = "localhost"
    mport = 9998 # master port
    sport = 9999  # slave port
    cport = 11000 # client port
    
    master_path = "data/master/"
    slave_path = "data/slave/"
    node_path = "data/node/"

class DevConfig(Config):
    env = "DEV"

class ProdConfig(Config):
    env = "PROD"

config = {
    "DEV": DevConfig,
    "PROD": ProdConfig
}