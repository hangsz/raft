#!/usr/bin/env python
# coding: utf-8

import setuptools
from setuptools import setup

with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()

setup(
    name='raft-python',
    version='0.1.3',
    author='hangsz',
    author_email='zhenhang.sun@outlook.com',
    url='https://github.com/hangsz/raft',
    description=u'raft python实现, 用于测试学习',
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    install_requires=[
        'pydantic==1.10.7',
        'tomli==2.0.1',
        'typing_extensions==4.5.0'
    ]
)