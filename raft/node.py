#!/usr/bin/env python
# coding: utf-8
'''
@File    :   node.py
@Time    :   2022/03/19 20:40:29
@Author  :   https://github.com/hangsz
@Version :   0.1.0
@Contact :   zhenhang.sun@gmail.com
'''

import json
import logging
import os
import random
import sys
import time
import traceback

from . import config, rpc
from .log import Log

logger = logging.getLogger(__name__)
logger.propagate = False

class Node(object):
    """raft node
    """

    def __init__(self, meta: dict):
        """

        Args:
            meta (dict): node meta
        """
        self.role = 'follower'
        
        self.group_id = meta['group_id']
        self.id = meta['id']
        self.addr = meta['addr']
        self.peers = meta['peers']
        
        self.conf = config.load_conf()
        self.path = self.conf.NODE_PATH
       
        os.makedirs(self.path, exist_ok=True)

        # persistent state
        self.current_term = 0
        self.voted_for = None
        self.persistent_filename = os.path.join(self.path, self.group_id+'_'+self.id+'_persistent.json')

        # init persistent state
        self.load()
        
        logname = os.path.join(self.path, self.group_id+'_'+self.id+'_log.json')
        self.log = Log(logname)

        # volatile state
        # rule 1, 2
        self.commit_index = 0
        self.last_applied = 0

        # volatile state on leaders
        # rule 1, 2
        self.next_index = {_id: self.log.last_log_index + 1 for _id in self.peers}
        self.match_index = {_id: -1 for _id in self.peers}

        # append entries
        self.leader_id = None

        # request vote
        self.vote_ids = {_id: 0 for _id in self.peers}

        # client request
        self.client_addr = None

        # tick
        self.wait_ms = (10, 20)
        self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)
        self.next_heartbeat_time = 0

        # rpc
        self.rpc_endpoint = rpc.Endpoint(self.addr, timeout=2)

        # log
        handler = logging.FileHandler(os.path.join(self.path, self.group_id+'_'+self.id+'.log'), 'a')
        fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(funcName)s [line:%(lineno)d] %(message)s')
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    

    def load(self):
        filename = self.persistent_filename

        if not os.path.exists(filename):
            self.save()
        
        with open(filename, 'r') as f:
            persistent = json.load(f)
            self.current_term = persistent['current_term']
            self.voted_for = persistent['voted_for']

    def save(self):
        persistent = {'current_term': self.current_term,
                      'voted_for': self.voted_for}

        filename = self.persistent_filename
        with open(filename, 'w') as f:
            json.dump(persistent, f, indent=4)

    def redirect(self, data: dict, addr: tuple[str, int]) -> dict:
        """redirect to correct node

        Args:
            data (dict): 
            addr (tuple[str, int]): src (ip, port)

        Returns:
            dict: 
        """
        if not data:
            return {}

        if data.get('type') == 'client_append_entries':
            if self.role != 'leader':
                if self.leader_id:
                    logger.info(f"redirect client_append_entries to leader: {self.leader_id}")
                    self.rpc_endpoint.send(data, self.peers.get(self.leader_id))
                return {}
            else:
                self.client_addr = (addr[0], self.conf.CLIENT_PORT)
                # logger.info("client addr " + self.client_addr[0] +'_' +str(self.client_addr[1]))
                return data
        
        if data.get('dst_id') != self.id:
            logger.info(f"redirect to: {data.get('dst_id')}")
            self.rpc_endpoint.send(data, self.peers.get(data.get('dst_id')))
            return {}


        return data

    def append_entries(self, data: dict) -> bool:
        """append entries rpc. only used in follower state

        Args:
            data (dict): 

        Returns:
            bool: 
        """
        response = {'type': 'append_entries_response',
                    'src_id': self.id,
                    'dst_id': data.get('src_id'),
                    'term': self.current_term,
                    'success': False}

        # append_entries rule 1
        if data.get('term') < self.current_term:
            logger.info('1. success = False: smaller term')
            logger.info(f"  send append_entries_response to leader: {data.get('src_id')}")
            response['success'] = False
            self.rpc_endpoint.send(response, self.peers.get(data.get('src_id')))
            return False
       
        if not data.get("entries"):
            logger.info("heartbeat")
        else:
            # append_entries rule 2, 3
            prev_log_index = data.get('prev_log_index', -1)
            prev_log_term = data.get('prev_log_term')

            if prev_log_term != self.log.get_log_term(prev_log_index):
                logger.info('2. success = False: index not match or term not match')
                response['success'] = False
                logger.info('3. log delete_entries')
                self.log.delete_entries(prev_log_index)
            else:
            # append_entries rule 4
                logger.info('4. success = True')
                logger.info('   log append_entries')
                response['success'] = True
                self.log.append_entries(prev_log_index, data.get('entries', []))

            logger.info(f"   send append_entries_response to leader: {data.get('src_id')}")
            self.rpc_endpoint.send(response, self.peers.get(data.get('src_id')))

        # append_entries rule 5
        leader_commit = data.get('leader_commit')
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.log.last_log_index)
            logger.info(f"5. commit_index = {str(self.commit_index)}")
        
        # set leader_id
        self.leader_id = data.get('leader_id')

        return True

    def request_vote(self, data: dict) -> bool:
        """request vote rpc. only used in follower state

        Args:
            data (dict): 

        Returns:
            bool: 
        """
        response = {'type': 'request_vote_response',
                    'src_id': self.id,
                    'dst_id': data['src_id'],
                    'term': self.current_term,
                    'vote_granted': False}

        # request_vote rule 1
        if data.get('term') < self.current_term:
            logger.info( '1. success = False: smaller term')
            logger.info(f"   send request_vote_response to candidate: {data.get('src_id')}")
            response['vote_granted'] = False
            self.rpc_endpoint.send(response, self.peers.get(data.get('src_id')))
            return False
        
        # request_vote rule 2
        last_log_index = data.get('last_log_index')
        last_log_term = data.get('last_log_term')

        if self.voted_for is None or self.voted_for == data.get('candidate_id'):
            if last_log_index >= self.log.last_log_index and last_log_term >= self.log.last_log_term:
                self.voted_for = data.get('src_id')
                self.save() 
                response['vote_granted'] = True
                logger.info( '2. success = True: candidate log is newer')
                logger.info(f"   send request_vote_response to candidate: {data['src_id']}")
                self.rpc_endpoint.send(response, self.peers.get(data.get('src_id')))

                return True
            else:
                self.voted_for = None
                self.save()
                response['vote_granted'] = False
                logger.info('2. success = False: candidate log is older')
                logger.info(f"   send request_vote_response to candidate: {data['src_id']}")
                self.rpc_endpoint.send(response, self.peers.get(data.get('src_id')))
                return False
        else:
            response['vote_granted'] = False
            logger.info(f"2. success = False: has vated for: {self.voted_for}")

            return True

    def all_do(self, data: dict):
        """all rule 1, 2

        Args:
            data (dict):
        """
        logger.info(f"all {self.id}".center(100, '-'))
        # all rule 1
        if self.commit_index > self.last_applied:
            self.last_applied = self.commit_index
            logger.info(f"1. last_applied = {str(self.last_applied)}")
            logger.info(f"   attention: need to apply to state machine")

        # all rule 2
        if data.get('term', -1) > self.current_term:
            logger.info( "2. become follower")
            logger.info(f"   receive bigger term: {data.get('term')} > {self.current_term}")
            self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)
            self.role = 'follower'
            self.current_term = data.get('term')
            self.voted_for = None
            self.save()
            self.leader_id = None

    def follower_do(self, data: dict) -> bool:
        """rules for servers: follower

        Args:
            data (dict):
        
        Returns:
            bool: 
        """
        logger.info(f"follower {self.id}".center(100, '-'))
        reset = False

        # follower rule 1
        if data.get('type') == 'append_entries':
            logger.info( "1. append_entries")
            logger.info(f"   receive from leader: {data.get('src_id')}")
            reset = self.append_entries(data)

        elif data.get('type') == 'request_vote':
            logger.info( "1. request_vote")
            logger.info(f"   recevive from candidate: {data.get('src_id')}")
            reset = self.request_vote(data)
        
        # reset next_leader_election_time
        if reset: 
            logger.info('   reset next_leader_election_time')
            self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)
        
        # follower rule 2
        if time.time() > self.next_leader_election_time:
            logger.info('1. become candidate')
            logger.info('   no request from leader or candidate')
            self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)
            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.id
            self.save()
            self.leader_id = None
            self.vote_ids = {_id: False for _id in self.peers}
            
            return True

    def candidate_do(self, data: dict) -> bool:
        """rules for servers: candidate

        Args:
            data (dict):
        
        Returns:
            bool: 
        """
        logger.info(f"candidate {self.id}".center(100, '-'))
        # candidate rule 1
        for dst_id in self.peers:
            # if self.vote_ids.get(dst_id):
            #     continue
            request = {
                'type': 'request_vote',
                'src_id': self.id,
                'dst_id': dst_id,
                'term': self.current_term,
                'candidate_id': self.id,
                'last_log_index': self.log.last_log_index,
                'last_log_term': self.log.last_log_term
            }
            logger.info(f"1. send request_vote request to peer: {dst_id}")
            self.rpc_endpoint.send(request, self.peers.get(dst_id))

        # candidate rule 2
        if data.get('type') == 'request_vote_response':
            logger.info(f"1. receive request_vote_response from follower: {data.get('src_id')}")
            self.vote_ids[data.get('src_id')] = data.get('vote_granted')
            vote_count = sum(list(self.vote_ids.values()))

            if vote_count >= len(self.peers)//2:
                logger.info('2. become leader: get enougth vote')
                self.role = 'leader'
                self.voted_for = None
                self.save()
                self.next_heartbeat_time = 0
                self.next_index = {_id: self.log.last_log_index + 1 for _id in self.peers}
                self.match_index = {_id: 0 for _id in self.peers}
                
                return True

        # candidate rule 3
        elif data.get('type') == 'append_entries':
            logger.info(f"1. receive append_entries request from leader: {data.get('src_id')}")
            logger.info('2. become follower')
            self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)
            self.role = 'follower'
            self.voted_for = None
            self.save()
            return 

        # candidate rules: rule 4
        if time.time() > self.next_leader_election_time:
            logger.info('candidate: 1. leader_election timeout')
            logger.info('           2. become candidate')
            self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)
            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.id
            self.save()
            self.vote_ids = {_id: False for _id in self.peers}

            return

    def leader_do(self, data: dict):
        '''rules for leader

        Args:
            data (dict):
        '''
        logger.info(f"leader {self.id}".center(100, '-'))
        # leader rule 1, 3
        if  time.time() > self.next_heartbeat_time:
            self.next_heartbeat_time = time.time() + random.randint(0, 5)

            for dst_id in self.peers:
                request = {'type': 'append_entries',
                           'src_id': self.id,
                           'dst_id': dst_id,
                           'term': self.current_term,
                           'leader_id': self.id,
                           'prev_log_index': self.next_index[dst_id] - 1,
                           'prev_log_term': self.log.get_log_term(self.next_index[dst_id]-1),
                           'entries': self.log.get_entries(self.next_index[dst_id]),
                           'leader_commit': self.commit_index}
                
                logger.info(f"1. send append_entries to peer: {dst_id}")
                self.rpc_endpoint.send(request, self.peers.get(dst_id))

        # leader rule 2
        if  data.get('type') == 'client_append_entries':
            data['term'] = self.current_term
            self.log.append_entries(self.log.last_log_index, [data])

            logger.info('2. receive append_entries from client')
            logger.info('   log append_entries')
            logger.info('   log save')

            return

        # leader rule 3
        if data.get('type') == 'append_entries_response':
            logger.info(f"1. receive append_entries_response from follower: {data.get('src_id')}")
            if data.get('success') == False:
                self.next_index[data.get('src_id')] -= 1
                logger.info('2. success = False, next_index - 1')
            else:
                self.match_index[data.get('src_id')] = self.next_index.get(data.get('src_id'))
                self.next_index[data.get('src_id')] = self.log.last_log_index + 1
                logger.info(' 2. success = True')
                logger.info(f"  next_index = {str(self.next_index.get(data.get('src_id')))}")
                logger.info(f"  match_index = {str(self.match_index.get(data.get('src_id')))}")

        # leader rule 4
        while True:
            N = self.commit_index + 1
            count = 0
            for _id in self.match_index:
                if self.match_index[_id] >= N:
                    count += 1
                if count >= len(self.peers)//2:
                    self.commit_index = N
                    logger.info('4. commit + 1')

                    if self.client_addr:
                        response = {'index': self.commit_index}
                        self.rpc_endpoint.send(response, self.client_addr)
                    break
            else:
                logger.info(f"4. commit = {str(self.commit_index)}")
                break


    def run(self):
        while True:
            try:
                try:
                    data, addr = self.rpc_endpoint.recv()
                except Exception as e:
                    data, addr = {}, None

                data = self.redirect(data, addr)
                self.all_do(data)
                
                if self.role == 'follower':
                    if self.follower_do(data):
                        continue

                if self.role == 'candidate':
                    if self.candidate_do(data):
                        continue

                if self.role == 'leader':
                    self.leader_do(data)
            
            except SystemExit:
                sys.exit("sorry, something wrong")
 
            except KeyboardInterrupt:
                self.rpc_endpoint.close()
                sys.exit(0)
            except Exception:
                traceback.print_exc()
                logger.info(traceback.format_exc())


def main() -> int:
    meta = {"group_id": "2",
            "id": "0",
            "addr": ("localhost", 10000),
            "peers": {"1": ("localhost", 10001), "2": ("localhost", 10002)}}

    node = Node(meta)

    node.run()

if __name__ == "__main__":
    sys.exit(main())