__author__ = 'zhenhang.sun@gmail.com'
__version__ = '1.0.0'

class Config(object):
    ip = "localhost"
    cport = 9999  # control plane port
    
    master_path = "data/master/"
    slave_path = "data/slave/"
    node_path = "data/node/"
    log_path = "data/log/"



class DevConfig(Config):
    env = "DEV"

class ProdConfig(Config):
    env = "PROD"

config = {
    "DEV": DevConfig,
    "PROD": ProdConfig
}