import time
import random
import threading
import requests
from flask import Flask, request, jsonify, redirect
import math
import sys
import uuid

ELECTION_TIME_FROM = 1.5
ELECTION_TIME_TO = 7
HEARTBEAT_INTERVAL = 0.5

ACKNOWLEDGE_SLEEP_INTERVAL = 0.1

BASE_PORT = 6666

REPLICAS = list(range(4))

app = Flask(__name__)

class RaftNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.replicas = REPLICAS
        
        self.term = 0
        self.voted_for = None
        self.log = []
        self.commit_length = 0
        self.db = {}

        self.role = 'follower'
        self.leader: int = -1
        self.votes_received = {}
        self.sent_length = dict()
        self.acked_length = dict()

        self.election_timeout = random.uniform(ELECTION_TIME_FROM, ELECTION_TIME_TO)
        self.last_heartbeat = time.time()
        self.heartbeat_interval = HEARTBEAT_INTERVAL

        threading.Thread(target=self.election_timer, daemon=True).start()
        threading.Thread(target=self.send_heartbeat, daemon=True).start()



######## CLIENT HANDLERS ############

    def handle_client_action(self, key, value, action):
        if self.role != 'leader':
            return self.leader

        self.log.append({'term': self.term, 'key': key, 'value': value, 'action': action})
        self.acked_length[self.node_id] = len(self.log)

        for follower in self.replicas:
            if follower != self.node_id:
                self.replicate_logs_to_replica(self.node_id, follower)
        return -1
    

    # лень обобщать
    def handle_client_cas(self, key, old_value, new_value):
        if self.role != 'leader':
            return int(self.leader)
        
        guid = str(uuid.uuid4())
        self.log.append({'term': self.term, 'key': key, 'action': 'cas', 'old_value': old_value, 'new_value': new_value, 'guid': guid})
        self.acked_length[self.node_id] = len(self.log)

        for follower in self.replicas:
            if follower != self.node_id:
                self.replicate_logs_to_replica(self.node_id, follower)
        return guid



######## RAFT HANDLERS ##########

    def logs_request_handler(self, data):
        leader_id = int(data['leader_id'])
        term = data['term']
        log_length = data['log_length']
        entries = data['entries']

        if term > self.term or (term == self.term and self.role == 'candidate'):
            self.term = max(term, self.term)
            self.role = 'follower'
            self.leader = leader_id
            self.voted_for = None
            self.last_heartbeat = time.time()
        
        log_ok = (len(self.log) >= log_length) and (log_length == 0 or data['prev_log_term'] == self.log[log_length - 1]['term'])

        if term == self.term and log_ok:
            self.leader = leader_id
            self.voted_for = None
            self.last_heartbeat = time.time()
            self.append_entries(log_length, data['commit_length'], entries)

        payload = {
            'node_id': self.node_id,
            'current_term': self.term,
            'ack': log_length + len(entries) if log_ok and self.term == term else 0,
            'status': log_ok and self.term == term,
        }
        self.send_post(f'http://localhost:{BASE_PORT + leader_id}/raft/log_response', payload)


    def logs_response_handler(self, data):
        follower = data['node_id']
        term = data['current_term']
        ack = data['ack']

        if term == self.term and self.role == 'leader':
            if data['status'] and ack >= self.acked_length[follower]:
                self.sent_length[follower] = ack
                self.acked_length[follower] = ack
                self.commit_log_entries()
            elif self.sent_length[follower] > 0:
                self.sent_length[follower] = self.sent_length[follower] - 1
                self.replicate_logs_to_replica(self.node_id, follower)
        elif term > self.term:
            self.term = term
            self.role = 'follower'
            self.voted_for = None


    def response_vote_handler(self, data):
        term = data['term']
        if self.role == 'candidate' and term == self.term and data['vote_granted']:
            self.votes_received.add(data['node_id'])
            if len(self.votes_received) >= math.ceil((len(self.replicas) + 1) / 2):
                self.become_leader()
        elif term > self.term:
            self.term = term
            self.role = 'follower'
            self.voted_for = None


    def request_vote_handler(self, data):
        c_id = data['node_id']
        c_term = data['term']
        c_log_length = data['log_length']
        c_log_term = data['last_term']

        my_log_term = self.log[len(self.log) - 1]['term'] if len(self.log) > 0 else 0

        log_ok = (c_log_term > my_log_term) or (c_log_term == my_log_term and c_log_length >= len(self.log))
        term_ok = (c_term > self.term) or (c_term == self.term and (self.voted_for is None or self.voted_for == c_id))

        if log_ok and term_ok:
            self.term = c_term
            self.role = 'follower'
            self.voted_for = c_id

        msg = {
            'node_id': self.node_id,
            'term': self.term,
            'vote_granted': log_ok and term_ok,
        }
        self.send_post(f'http://localhost:{BASE_PORT + c_id}/raft/response_vote', msg)
    


######## RAFT LOGIC ###########

    def append_entries(self, log_length, leader_commit, entries):
        if len(entries) > 0 and len(self.log) > log_length:
            if self.log[log_length]['term'] != entries[0]['term']:
                self.log = self.log[0:len(log_length)]
        if log_length + len(entries) > len(self.log):
            for i in range(len(self.log) - log_length, len(entries)):
                self.log.append(entries[i]) 
        if leader_commit > self.commit_length:
            for i in range(self.commit_length, leader_commit):
                self.crud(i)
            self.commit_length = leader_commit


    def get_max_ready(self, min_acks) -> int:
        for log_index in range(len(self.log), 0, -1):
            if sum([key for key in self.acked_length if self.acked_length[key] >= log_index]) >= min_acks:
                return log_index
        return 0
    

    def commit_log_entries(self):
        max_ready = self.get_max_ready(math.ceil((len(self.replicas) + 1) / 2))
        if max_ready != 0 and max_ready >= self.commit_length and self.log[max_ready - 1]['term'] == self.term:
            for i in range(self.commit_length, max_ready):
                self.crud(i)
            self.commit_length = max_ready
    

    def crud(self, log_index):
        key = self.log[log_index]['key']
        if self.log[log_index]['action'] in ['create', 'update']:
            value = self.log[log_index]['value']
            self.db[key] = value
        if self.log[log_index]['action'] == 'delete':
            self.db.pop(key)
        if self.log[log_index]['action'] == 'cas':
            guid = self.log[log_index]['guid']
            self.db[guid] = self.db[key] == self.log[log_index]['old_value']
            if self.db[guid]: self.db[key] = self.log[log_index]['new_value']
            


####### ELECTION STUFF ##########

    def send_heartbeat(self):
        while True:
            if self.role == 'leader':
                for follower in self.replicas:
                    if follower != self.node_id:
                        self.replicate_logs_to_replica(self.node_id, follower)
            time.sleep(self.heartbeat_interval)

    
    def election_timer(self):
        while True:
            if self.role == 'follower' and (time.time() - self.last_heartbeat > self.election_timeout):
                self.last_heartbeat = time.time() + self.election_timeout
                self.start_election()
            time.sleep(self.heartbeat_interval)
    

    def start_election(self):
        self.term += 1
        self.role = 'candidate'
        self.voted_for = self.node_id

        self.votes_received = {self.node_id}

        msg = {
            'node_id': self.node_id,
            'term': self.term,
            'log_length': len(self.log),
            'last_term': self.log[len(self.log) - 1]['term'] if len(self.log) > 0 else 0
        }

        print(f'[Node {self.node_id}] Starting election for term {self.term}')

        for id in self.replicas:
            if id != self.node_id:
                self.send_post(f'http://localhost:{BASE_PORT + id}/raft/request_vote', msg)
    
    
    def become_leader(self):
        self.role = 'leader'
        self.leader = self.node_id
        print(f'[Node {self.node_id}] Became leader for term {self.term}')
        for follower in self.replicas:
            if follower != self.node_id:
                self.sent_length[follower] = len(self.log)
                self.acked_length[follower] = 0
                self.replicate_logs_to_replica(self.node_id, follower)



######## MSG STUFF #########

    def replicate_logs_to_replica(self, leader_id, follower_id):
        i = self.sent_length[follower_id]

        payload = {
            'leader_id': leader_id,
            'term': self.term,
            'log_length': i,
            'prev_log_term': self.log[i - 1]['term'] if i > 0 else 0,
            'commit_length': self.commit_length,
            'entries': self.log[i:len(self.log)]
        }
        self.send_post(f'http://localhost:{BASE_PORT + follower_id}/raft/log_request', payload)
    
    
    def send_post(self, url, msg):
        try:
            requests.post(url, json=msg, timeout=0.0001)
        except requests.exceptions.RequestException as e:
            pass



######### RAFT FLASK #############

node: RaftNode = None

def handle_raft(method):
    method(request.get_json())
    return '', 204

@app.route('/raft/log_request', methods=['POST'])
def log_request():
    return handle_raft(node.logs_request_handler)

@app.route('/raft/log_response', methods=['POST'])
def log_response():
    return handle_raft(node.logs_response_handler)

@app.route('/raft/request_vote', methods=['POST'])
def request_vote():
    return handle_raft(node.request_vote_handler)

@app.route('/raft/response_vote', methods=['POST'])
def response_vote():
    return handle_raft(node.response_vote_handler)

######### CRUD FLASK #############

@app.route('/client', methods=['POST'])
def create():
    key = request.json.get('key')
    leader_id = node.handle_client_action(key, request.json.get('value'), 'create')
    if leader_id != -1:
        return redirect(f'http://localhost:{BASE_PORT + leader_id}/client', code=307)
    while True:
        if key in node.db:
            return jsonify({'key': key, 'value': node.db[key]}), 201
        time.sleep(ACKNOWLEDGE_SLEEP_INTERVAL)
        
@app.route('/client/<key>', methods=['GET', 'PUT', 'DELETE'])
def RUD(key):
    if key not in node.db:
        return 'Not found', 404
    if request.method == 'GET':
        return jsonify({'key': key, 'value': node.db[key]}), 200
    method = 'update' if request.method == 'PUT' else 'delete'
    leader_id = node.handle_client_action(key, request.json.get('value') if method == 'update' else node.db[key], method)
    if leader_id != -1:
        return redirect(f'http://localhost:{BASE_PORT + leader_id}/client/{key}', code=307)
    return '', 204

@app.route('/client/cas/<key>', methods=['PATCH'])
def cas(key):
    if key not in node.db:
        return 'Not found', 404
    old_value = request.json.get('old_value')
    new_value = request.json.get('new_value')

    data = node.handle_client_cas(key, old_value, new_value)
    # If returned leader_id
    if isinstance(data, int):
        return redirect(f'http://localhost:{BASE_PORT + data}/client/cas/{key}', code=307)
    # If returned guid
    while True:
        if data in node.db:
            return jsonify({'status': node.db.get(data)}), 200
        time.sleep(ACKNOWLEDGE_SLEEP_INTERVAL)


if __name__ == '__main__':
    node = RaftNode(int(sys.argv[1]))
    app.run(host='0.0.0.0', port=BASE_PORT + node.node_id)