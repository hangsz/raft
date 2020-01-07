# 说明

非拜占庭节点的分布式共识算法Raft的python实现。

Raft官网：https://raft.github.io/


## raft
- node.py, log.py为核心代码。
- node.py实现raft的节点消息收发、角色转换等功能。
- log.py实现具体的日志存储。

## test
1. 进入test目录。
2. node1.py, node2.py, node3.py为三个节点，可以直接 python 运行。
3. client.py文件为客户端向raft节点发送数据，可以直接 python 运行。
4. 会自动生成node1, node2, node3目录存储数据。
5. 可以用compare 软件进行文件一致性比较。
