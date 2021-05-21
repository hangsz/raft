# coding: utf-8

__author__ = 'zhenhang.sun@gmail.com'
__version__ = '1.0.0'

from genericpath import exists
import os
import json
import time
import random
import logging

from .log import Log
from .rpc import Rpc
from .config import config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(funcName)s [line:%(lineno)d]\n%(message)s')
logger = logging.getLogger(__name__)

env = os.environ.get("env")
conf = config[env] if env else config['DEV']

class Node(object):
    """
    raft node
    """

    def __init__(self, meta):
        self.role = 'follower'
        
        self.group_id = meta['group_id']
        self.id = meta['id']
        self.addr = meta['addr']
        self.peers = meta['peers']

        self.path = conf.node_path
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        # persistent state
        self.current_term = 0
        self.voted_for = None

        # init persistent state
        self.load()
        self.log = Log(self.id)

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
        self.rpc_endpoint = Rpc(self.addr)

    def load(self):
        filename = self.path + self.group_id + "_" + self.id + '_persistent.json'
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                data = json.load(f)

            self.current_term = data['current_term']
            self.voted_for = data['voted_for']

        else:
            self.save()

    def save(self):
        data = {'current_term': self.current_term,
                'voted_for': self.voted_for,
                }

        filename = self.path + self.group_id + "_" + self.id + '_persistent.json'
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)

    def redirect(self, data, addr):
        if data == None:
            return None

        if data['type'] == 'client_append_entries':
            if self.role != 'leader':
                if self.leader_id:
                    logger.info('redirect: client_append_entries to leader')
                    self.rpc_endpoint.send(data, self.peers[self.leader_id])
                return None
            else:
                self.client_addr = addr
                return data

        if data['dst_id'] != self.id:
            logger.info('redirect: to ' + data['dst_id'])
            # logger.info('redirec to leader')
            self.rpc_endpoint.send(data, self.peers[data['dst_id']])
            return None
        else:
            return data

        return data

    def append_entries(self, data):
        '''
        append entries rpc
        only used in follower state
        '''
        response = {'type': 'append_entries_response',
                    'src_id': self.id,
                    'dst_id': data['src_id'],
                    'term': self.current_term,
                    'success': False
                    }

        # append_entries: rule 1
        if data['term'] < self.current_term:
            logger.info('          2. smaller term')
            logger.info('          3. success = False: smaller term')
            logger.info('          4. send append_entries_response to leader ' + data['src_id'])
            response['success'] = False
            self.rpc_endpoint.send(response, self.peers[data['src_id']])
            return


        self.leader_id = data['leader_id']

        # heartbeat
        if data['entries'] == []:
            logger.info('          4. heartbeat')
            return

        prev_log_index = data['prev_log_index']
        prev_log_term = data['prev_log_term']
        
        tmp_prev_log_term = self.log.get_log_term(prev_log_index)

        # append_entries: rule 2, 3
        # append_entries: rule 3
        if tmp_prev_log_term != prev_log_term:
            logger.info('          4. success = False: index not match or term not match')
            logger.info('          5. send append_entries_response to leader ' + data['src_id'])
            logger.info('          6. log delete_entries')
            logger.info('          6. log save')

            response['success'] = False
            self.rpc_endpoint.send(response, self.peers[data['src_id']])
            self.log.delete_entries(prev_log_index)

        # append_entries rule 4
        else:
            logger.info('          4. success = True')
            logger.info('          5. send append_entries_response to leader ' + data['src_id'])
            logger.info('          6. log append_entries')
            logger.info('          7. log save')

            response['success'] = True
            self.rpc_endpoint.send(response, self.peers[data['src_id']])
            self.log.append_entries(prev_log_index, data['entries'])

            # append_entries rule 5
            leader_commit = data['leader_commit']
            if leader_commit > self.commit_index:
                commit_index = min(leader_commit, self.log.last_log_index)
                self.commit_index = commit_index
                logger.info('          8. commit_index = ' + str(commit_index))

        return

    def request_vote(self, data):
        '''
        request vote rpc
        only used in follower state
        '''

        response = {'type': 'request_vote_response',
                    'src_id': self.id,
                    'dst_id': data['src_id'],
                    'term': self.current_term,
                    'vote_granted': False
                    }

        # request vote: rule 1
        if data['term'] < self.current_term:
            logger.info('          2. smaller term')
            logger.info('          3. success = False')
            logger.info('          4. send request_vote_response to candidate ' + data['src_id'])
            response['vote_granted'] = False
            self.rpc_endpoint.send(response, self.peers[data['src_id']])
            return

        logger.info('          2. same term')
        candidate_id = data['candidate_id']
        last_log_index = data['last_log_index']
        last_log_term = data['last_log_term']

        if self.voted_for == None or self.voted_for == candidate_id:
            if last_log_index >= self.log.last_log_index and last_log_term >= self.log.last_log_term:
                self.voted_for = data['src_id']
                self.save() 
                response['vote_granted'] = True
                self.rpc_endpoint.send(response, self.peers[data['src_id']])
                logger.info('          3. success = True: candidate log is newer')
                logger.info('          4. send request_vote_response to candidate ' + data['src_id'])
            else:
                self.voted_for = None
                self.save()
                response['vote_granted'] = False
                self.rpc_endpoint.send(response, self.peers[data['src_id']])
                logger.info('          3. success = False: candidate log is older')
                logger.info('          4. send request_vote_response to candidate ' + data['src_id'])
        else:
            response['vote_granted'] = False
            self.rpc_endpoint.send(response, self.peers[data['src_id']])
            logger.info('          3. success = False: has vated for ' + self.voted_for)
            logger.info('          4. send request_vote_response to candidate ' + data['src_id'])

        return

    def all_do(self, data):
        '''
        all servers: rule 1, 2
        '''

        logger.info('-------------------------------all------------------------------------------')
        

        if self.commit_index > self.last_applied:
            self.last_applied = self.commit_index
            logger.info('all: 1. last_applied = ' + str(self.last_applied))

        if data == None:
            return

        if data['type'] == 'client_append_entries':
            return

        if data['term'] > self.current_term:
            logger.info( f'all: 1. bigger term: { data["term"]} > {self.current_term}' )
            logger.info('     2. become follower')
            self.role = 'follower'
            self.current_term = data['term']
            self.voted_for = None
            self.save()

        return

    def follower_do(self, data):

        '''
        rules for servers: follower
        '''
        logger.info('-------------------------------follower-------------------------------------')

        t = time.time()
        # follower rules: rule 1
        if data != None:

            if data['type'] == 'append_entries':
                logger.info('follower: 1. recv append_entries from leader ' + data['src_id'])

                if data['term'] == self.current_term:
                    logger.info('          2. same term')
                    logger.info('          3. reset next_leader_election_time')
                    self.next_leader_election_time = t + random.randint(*self.wait_ms)
                self.append_entries(data)

            elif data['type'] == 'request_vote':
                logger.info('follower: 1. recv request_vote from candidate ' + data['src_id'])
                self.request_vote(data)

        # follower rules: rule 2
        if t > self.next_leader_election_time:
            logger.info('follower：1. become candidate')
            self.next_leader_election_time = t + random.randint(*self.wait_ms)
            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.id
            self.save()
            self.vote_ids = {_id: 0 for _id in self.peers}

        return

    def candidate_do(self, data):
        '''
        rules for fervers: candidate
        '''
        logger.info('-------------------------------candidate------------------------------------')
        
        t = time.time()
        # candidate rules: rule 1
        for dst_id in self.peers:
            if self.vote_ids[dst_id] == 0:
                logger.info('candidate: 1. send request_vote to peer ' + dst_id)
                request = {
                    'type': 'request_vote',
                    'src_id': self.id,
                    'dst_id': dst_id,
                    'term': self.current_term,
                    'candidate_id': self.id,
                    'last_log_index': self.log.last_log_index,
                    'last_log_term': self.log.last_log_term
                }
                # logger.info(request)

                self.rpc_endpoint.send(request, self.peers[dst_id])
        
        # if data != None and data['term'] < self.current_term:
        #     logger.info('candidate: 1. smaller term from ' + data['src_id'])
        #     logger.info('           2. ignore')
            # return

        if data != None and data['term'] == self.current_term:
            # candidate rules: rule 2
            if data['type'] == 'request_vote_response':
                logger.info('candidate: 1. recv request_vote_response from follower ' + data['src_id'])

                self.vote_ids[data['src_id']] = data['vote_granted']
                vote_count = sum(list(self.vote_ids.values()))

                if vote_count >= len(self.peers)//2:
                    logger.info('           2. become leader')
                    self.role = 'leader'
                    self.voted_for = None
                    self.save()
                    self.next_heartbeat_time = 0
                    self.next_index = {_id: self.log.last_log_index + 1 for _id in self.peers}
                    self.match_index = {_id: 0 for _id in self.peers}
                    return

            # candidate rules: rule 3
            elif data['type'] == 'append_entries':
                logger.info('candidate: 1. recv append_entries from leader ' + data['src_id'])
                logger.info('           2. become follower')
                self.next_leader_election_time = t + random.randint(*self.wait_ms)
                self.role = 'follower'
                self.voted_for = None
                self.save()
                return

        # candidate rules: rule 4
        if t > self.next_leader_election_time:
            logger.info('candidate: 1. leader_election timeout')
            logger.info('           2. become candidate')
            self.next_leader_election_time = t + random.randint(*self.wait_ms)
            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.id
            self.save()
            self.vote_ids = {_id: 0 for _id in self.peers}
            return

    def leader_do(self, data):
        '''
        rules for fervers: leader
        '''
        logger.info('-------------------------------leader---------------------------------------')

        # leader rules: rule 1, 3
        t = time.time()
        if t > self.next_heartbeat_time:
            self.next_heartbeat_time = t + random.randint(0, 5)

            for dst_id in self.peers:
                logger.info('leader：1. send append_entries to peer ' + dst_id)

                request = {'type': 'append_entries',
                           'src_id': self.id,
                           'dst_id': dst_id,
                           'term': self.current_term,
                           'leader_id': self.id,
                           'prev_log_index': self.next_index[dst_id] - 1,
                           'prev_log_term': self.log.get_log_term(self.next_index[dst_id] - 1),
                           'entries': self.log.get_entries(self.next_index[dst_id]),
                           'leader_commit': self.commit_index
                           }

                self.rpc_endpoint.send(request, self.peers[dst_id])

        # leader rules: rule 2
        if data != None and data['type'] == 'client_append_entries':
            data['term'] = self.current_term
            self.log.append_entries(self.log.last_log_index, [data])

            logger.info('leader：1. recv append_entries from client')
            logger.info('        2. log append_entries')
            logger.info('        3. log save')

            return

        # leader rules: rule 3.1, 3.2
        if data != None and data['term'] == self.current_term:
            if data['type'] == 'append_entries_response':
                logger.info('leader：1. recv append_entries_response from follower ' + data['src_id'])
                if data['success'] == False:
                    self.next_index[data['src_id']] -= 1
                    logger.info('        2. success = False')
                    logger.info('        3. next_index - 1')
                else:
                    self.match_index[data['src_id']] = self.next_index[data['src_id']]
                    self.next_index[data['src_id']] = self.log.last_log_index + 1
                    logger.info('        2. success = True')
                    logger.info('        3. match_index = ' + str(self.match_index[data['src_id']]) +  ' next_index = ' + str(self.next_index[data['src_id']]))

        # leader rules: rule 4
        while True:
            N = self.commit_index + 1

            count = 0
            for _id in self.match_index:
                if self.match_index[_id] >= N:
                    count += 1
                if count >= len(self.peers)//2:
                    self.commit_index = N
                    logger.info('leader：1. commit + 1')

                    if self.client_addr:
                        response = {'index': self.commit_index}
                        self.rpc_endpoint.send(response, (self.client_addr[0],10000))

                    break
            else:
                logger.info('leader：2. commit = ' + str(self.commit_index))
                break


    def run(self):

        data = {
            "type": "create_node_success",
            "group_id": self.group_id,
            "id": self.id
        }
        self.rpc_endpoint.send(data, (conf.ip, conf.cport))

        data = {
            "type": "create_group_node_success",
            "group_id": self.group_id,
            "id": self.id
        }

        self.rpc_endpoint.send(data, (conf.ip, conf.cport))

        while True:
            try:
                try:
                    data, addr = self.rpc_endpoint.recv()
                except Exception as e:
                    # logger.info(e)
                    data, addr = None, None

                data = self.redirect(data, addr)

                self.all_do(data)

                if self.role == 'follower':
                    self.follower_do(data)

                if self.role == 'candidate':
                    self.candidate_do(data)

                if self.role == 'leader':
                    self.leader_do(data)

            except Exception as e:
                logger.info(e)

        self.ss.close()
        # self.cs.close()