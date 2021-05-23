__author__ = 'zhenhang.sun@gmail.com'
__version__ = '1.0.0'

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