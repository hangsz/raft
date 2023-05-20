#!/usr/bin/env python
# coding: utf-8
"""
@File    :   rpc.py
@Time    :   2022/03/19 14:46:35
@Author  :   https://github.com/hangsz
@Version :   0.1.0
@Contact :   zhenhang.sun@gmail.com
"""

import json
import logging
import socket
import sys
import traceback

logger = logging.getLogger(__name__)


class Endpoint(object):
    def __init__(self, addr: tuple[str, int] = None, timeout: int = None):
        """
        Args:
            addr (tuple[str, int], optional):  ip and port. Defaults to None.
            timeout (int, optional): . Defaults to None.
        """

        self.ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if addr:
            self.bind(tuple(addr))
        if timeout:
            self.ss.settimeout(timeout)

    def bind(self, addr: tuple[str, int]):
        """bind addr

        Args:
            addr (tuple[str, int]): ip and port
        """
        self.addr = tuple(addr)

        try:
            self.ss.bind(addr)
        except Exception:
            error = "exception: Rpc.bind exception"
            logger.exception(error)
            logger.exception(traceback.format_exc())
            sys.exit(error)

    def settimeout(self, timeout: int):
        """set timeout

        Args:
            timeout (int):
        """

        try:
            self.ss.settimeout(timeout)
        except Exception:
            error = "exception: Rpc.settimeout exception"
            logger.exception(error)
            logger.exception(traceback.format_exc())
            sys.exit(error)

    def send(self, data: dict, addr: tuple[str, int] = None):
        """send data

        Args:
            data (dict):
            ddr (tuple[str, int], optional): dst ip and port. Defaults to None.
        """
        if addr:
            data = json.dumps(data).encode("utf-8")
            self.ss.sendto(data, tuple(addr))

    def recv(
        self, addr: tuple[str, int] = None, timeout: int = None
    ) -> tuple[dict, tuple[str, int]]:
        """receive data
        Args:
            addr (tuple[str, int], optional): ip and port. Defaults to None.
            timeout (int, optional): . Defaults to None.

        Returns:
            tuple[dict, tuple[str, int]]: data and (ip, port)
        """
        if addr:
            self.bind(addr)

        if not self.addr:
            raise ("please bind to an addr")

        if timeout:
            self.settimeout(timeout)

        try:
            data, addr = self.ss.recvfrom(65535)
            data = json.loads(data)
        except TimeoutError:
            error = "exception: Rpc.recv timeout exception"
            return None, None
        except Exception:
            error = "exception: Rpc.recv unkonwn exception"
            logger.exception(error)
            logger.exception(traceback.format_exc())

            sys.exit(error)

        return data, addr

    def close(self):
        self.ss.close()
