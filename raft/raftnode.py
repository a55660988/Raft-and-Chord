import traceback
import raftnode_pb2 as rnp
import raftnode_pb2_grpc as rnpg
import requests
from netifaces import interfaces, ifaddresses, AF_INET
from enum import Enum
import random
import grpc
import os
import time
import threading
import sys
import pickle
import shutil
import json
'''
# TODO
1. Leader Election
2. Put Data
3. Persist Data
'''
VERBOSE = True


def mprint(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs)


class LogEntry(object):

    def __init__(self, _term=0, _key=None, _value=None, _uuid=None):
        self.term = _term
        self.key = _key
        self.value = _value
        self.uuid = _uuid

    def __repr__(self):
        return f'<Log term={self.term} key={self.key} value={self.value} />'

    def apply(self, store):
        store[self.key] = self.value


class State(Enum):
    CANDIDATE = 1
    FOLLOWER = 2
    LEADER = 3


def randomGenerate(s, t):
    return lambda: random.uniform(s, t)


class RaftNode(rnpg.raftnodeServicer):

    def __init__(self, _config, _port):

        self.config = _config
        self.port = _port

        self.initialize()

    def resetServer(self, req, ctx):
        shutil.rmtree(os.path.join(
            self.config['log_path']), ignore_errors=True)
        # data collection
        shutil.rmtree(os.path.join(
            self.config['data_path']), ignore_errors=True)
        return rnp.Null()

    def restartServer(self, req, ctx):
        try:
            self.timestamp(True, req.flag)
        except Exception as e:
            mprint(e)
        self.server.stop(0)
        os.execvp('python3', ['python3', 'server.py', str(self.port)])
        return rnp.Null()

    def logIndexFile(self, logIndex):
        return os.path.join(self.config['log_path'], 'log', str(logIndex))

    def getChannel(self, srvId):
        return grpc.insecure_channel(f"{self.config['servers'][srvId]['ip']}:{self.config['servers'][srvId]['port']}")

    def updateCommitIndex(self):
        for i in range(len(self.log)-1, self.commitIndex, -1):
            if len(list(filter(lambda x: x >= i, self.matchIndex))) >= self.majority and self.log[i].term == self.currentTerm:
                mprint("commit to index", i)
                self.commitIndex = i
                threading.Thread(target=self.updateLastApplied).start()
                break

    def updateLastApplied(self):
        with self.applyingLock:
            mprint("start apply from", self.lastApplied, self.commitIndex)
            while self.lastApplied < self.commitIndex and self.lastApplied+1 < len(self.log):
                self.log[self.lastApplied+1].apply(self.store)
                self.lastApplied += 1

    def changeToFollower(self, term=0):
        self.lastAppendEntriesTime = time.time()
        self.currentTerm = max(self.currentTerm, term)
        self.state = State.FOLLOWER
        mprint("change to follower, my term",
               self.currentTerm)
        self.writeData()

    def changeToCandidate(self):
        self.state = State.CANDIDATE
        self.writeData()

    def changeToLeader(self):
        self.timestamp()
        self.leaderId = self.id
        self.nextIndex = [len(self.log)] * self.srvCnt
        self.matchIndex = [0] * self.srvCnt
        self.state = State.LEADER
        self.writeData()

    def timestamp(self, restart=False, flag=False):
        if not flag:
            open(os.path.join(self.config['data_path'],
                              f'{int(restart)}-{self.currentTerm}-{time.time()}'), 'w+')

    def doFollower(self):
        while True:
            if self.state == State.FOLLOWER:
                # mprint("*** follower", time.time(),self.lastAppendEntriesTime)
                # self.votedFor = self.id
                if time.time() - self.lastAppendEntriesTime >= self.electionTimeout():
                    mprint("change to candidate due to timeout, my term",
                           self.currentTerm)
                    self.changeToCandidate()
            time.sleep(0.01)

    def doCandidate(self):
        def requestVote(srvId, results):
            try:
                with self.getChannel(srvId) as channel:
                    stub = rnpg.raftnodeStub(channel)
                    resp = stub.requestVote(rnp.requestVoteRequest(term=self.currentTerm,
                                                                   candidateId=self.id,
                                                                   lastLogIndex=len(
                                                                       self.log),
                                                                   lastLogTerm=self.log[-1].term,
                                                                   CMReq=True,
                                                                   srvId=self.id), timeout=10)
                    results[srvId] = resp
            except Exception as e:
                results[srvId] = e
        while True:
            timeout = self.electionTimeout()
            if self.state == State.CANDIDATE:
                self.currentTerm += 1
                mprint("*** candidate, my term", self.currentTerm)
                self.recvVote = {self.id}
                self.voteCnt = 1
                self.votedFor = self.id
                tid = [None] * self.srvCnt
                results = [None] * self.srvCnt
                while self.voteCnt < self.majority and len(self.recvVote) != self.srvCnt:
                    start_time = time.time()
                    for i in set(range(self.srvCnt)) - self.recvVote:
                        tid[i] = threading.Thread(
                            target=requestVote, args=(i, results))
                        tid[i].start()
                    for i in set(range(self.srvCnt)) - self.recvVote:
                        tid[i].join()
                        if isinstance(results[i], rnp.requestVoteResponse):
                            if results[i].code == rnp.GRANT:
                                self.voteCnt += 1
                            elif results[i].code == rnp.REJECT and results[i].term > self.currentTerm:
                                self.changeToFollower(results[i].term)
                                timeout = 0
                                break
                            if results[i].code != rnp.CM_FAIL:
                                self.recvVote.add(i)
                        # else:
                        #     mprint(results[i])
                    # check timeout
                    time.sleep(self.resendVoteTimeout())
                    timeout -= time.time() - start_time
                    if timeout <= 0:
                        break
                # mprint(results)
                mprint("voteCnt =", self.voteCnt, "recvVote =", self.recvVote)
                if self.state == State.CANDIDATE and self.voteCnt >= self.majority:
                    mprint("change to leader, my term", self.currentTerm)
                    self.changeToLeader()

            time.sleep(max(0, timeout))

    def doLeader(self):
        def appendEntries(srvId, results):
            try:
                entries = [rnp.Entry(term=x.term, key=x.key, value=x.value)
                           for x in self.log[self.nextIndex[srvId]:]]
                with self.getChannel(srvId) as channel:
                    stub = rnpg.raftnodeStub(channel)
                    resp = stub.appendEntries(rnp.appendEntriesRequest(term=self.currentTerm,
                                                                       leaderId=self.id,
                                                                       prevLogIndex=self.nextIndex[srvId]-1,
                                                                       prevLogTerm=self.log[self.nextIndex[srvId]-1].term,
                                                                       entries=entries,
                                                                       leaderCommit=self.commitIndex,
                                                                       CMReq=True,
                                                                       srvId=self.id), timeout=10)
                    results[srvId] = resp
            except Exception as e:
                results[srvId] = e

        while True:
            if self.state == State.LEADER:
                # mprint("*** leader")
                # heartbeat
                tid = [None] * self.srvCnt
                results = [None] * self.srvCnt
                mprint('hb,', end='')
                for i in range(self.srvCnt):
                    sys.stdout.flush()
                    if self.id != i:
                        tid[i] = threading.Thread(
                            target=appendEntries, args=(i, results))
                        tid[i].start()
                for i in range(self.srvCnt):
                    if self.id != i:
                        tid[i].join()
                        if isinstance(results[i], rnp.appendEntriesResponse):
                            if results[i].code == rnp.SUCCESS:
                                self.nextIndex[i] = len(self.log)
                                self.matchIndex[i] = len(self.log)-1
                                self.updateCommitIndex()
                            elif results[i].code == rnp.LOG_INCONSISTENT:
                                self.nextIndex[i] -= 1
                            elif results[i].code == rnp.REJECT:
                                mprint('hb rejected')
                                self.changeToFollower(results[i].term)
                        # else:
                        #     mprint(results[i])

                # mprint("leader do heartbeat", results)
                # join
            time.sleep(self.heartbeatTimeout())

    def setupID(self):
        self.id = -1
        publicip = requests.get('http://wgetip.com').text
        for i, srv in enumerate(self.config['servers']):
            if f"{srv['ip']}:{srv['port']}" == f"{publicip}:{self.port}":
                self.id = i
                return
            for interface in interfaces():
                for link in ifaddresses(interface).get(AF_INET, {}):
                    if f"{srv['ip']}:{srv['port']}" == f"{link['addr']}:{self.port}":
                        self.id = i
                        return

    def setupStubs(self):
        self.servers = [None] * self.srvCnt
        for i, srv in enumerate(self.config['servers']):
            if i != self.id:
                channel = grpc.insecure_channel(f"{srv['ip']}:{srv['port']}")
                self.servers[i] = rnpg.raftnodeStub(channel)

    def initialize(self):
        if 'verbose' in self.config:
            global VERBOSE
            VERBOSE = self.config['verbose']
        self.srvCnt = len(self.config['servers'])
        self.setupID()
        # self.setupStubs()
        self.majority = self.srvCnt // 2 + 1
        self.state = State.FOLLOWER
        self.leaderId = -1
        self.votedFor = self.id
        self.currentTerm = 0
        self.failrate = [self.config['failrate']] * self.srvCnt
        self.logLock = threading.Lock()
        self.log = [LogEntry()]
        self.logSet = {}

        self.store = {}

        # volatile
        self.commitIndex = 0
        self.lastApplied = 0
        self.applyingLock = threading.Lock()

        # leader
        self.nextIndex = [0] * self.srvCnt
        self.matchIndex = [0] * self.srvCnt

        self.recvVote = set()
        self.voteCnt = 0

        # timeouts
        # electionTimeout >> broadcastTime
        self.electionTimeout = randomGenerate(*self.config['electionTimeout'])
        self.heartbeatTimeout = randomGenerate(
            *self.config['heartbeatTimeout'])
        self.resendVoteTimeout = randomGenerate(
            *self.config['resendVoteTimeout'])
        self.networkTimeout = randomGenerate(*self.config['networkTimeout'])
        self.config['log_path'] = os.path.join(
            self.config['log_path'], str(self.id))
        try:
            oldState = self.readData()
            self.currentTerm = oldState['currentTerm']
            self.votedFor = oldState['votedFor']
            self.log += self.readAllLog()
        except Exception as e:
            mprint("init error", e)
        os.makedirs(os.path.join(
            self.config['log_path'], 'log'), exist_ok=True)
        # For collecting data
        self.config['data_path'] = os.path.join(
            self.config['data_path'], str(self.id))
        os.makedirs(os.path.join(self.config['data_path']), exist_ok=True)

    def isLeader(self, req, ctx):
        mprint("*** isLeader ***")
        if self.state == State.LEADER:
            return rnp.isLeaderResponse(isLeader=True, ip=req.ip)
        else:
            # need return its recent heard leader ip
            return rnp.isLeaderResponse(isLeader=False, ip="xxx")

    def getLeader(self, req, ctx):
        if self.leaderId != -1:
            srv = self.config['servers'][self.leaderId]
            return rnp.getLeaderResponse(ip=f"{srv['ip']}:{srv['port']}")
        else:
            return rnp.getLeaderResponse(ip="")
    # client call server

    def putValue(self, req, ctx):
        if self.state == State.LEADER:
            idx = -1
            if req.uuid not in self.logSet:
                with self.logLock:
                    self.log.append(LogEntry(self.currentTerm,
                                             req.key, req.value, req.uuid))
                    self.nextIndex[self.id] = len(self.log)
                    self.matchIndex[self.id] = self.nextIndex[self.id]-1
                    idx = len(self.log)-1
                    self.logSet[req.uuid] = idx
                    self.writeLog([len(self.log)-1])
                    mprint('put entry ', self.log[-1])
            else:
                idx = self.logSet[req.uuid]
            start = time.time()
            while self.commitIndex < idx and time.time()-start <= 0.05:
                time.sleep(0.01)

            return rnp.putValueResponse(code=rnp.SUCCESS)
        else:
            if self.leaderId == -1:
                mprint('no leader found before')
                return rnp.putValueResponse(code=rnp.NOT_LEADER)
            else:
                mprint('redirect ')
                srv = self.config['servers'][self.leaderId]
                return rnp.putValueResponse(code=rnp.NOT_LEADER, ip=f"{srv['ip']}:{srv['port']}")

    def getValue(self, req, ctx):
        if self.state == State.LEADER:
            if req.key in self.store:
                return rnp.getValueResponse(value=self.store[req.key], code=rnp.SUCCESS)
            else:
                return rnp.getValueResponse(code=rnp.KEY_NOT_EXIST)
        else:
            if self.leaderId == -1:
                mprint('no leader found before')
                return rnp.getValueResponse(code=rnp.NOT_LEADER)
            else:
                mprint('redirect ')
                srv = self.config['servers'][self.leaderId]
                return rnp.getValueResponse(code=rnp.NOT_LEADER, ip=f"{srv['ip']}:{srv['port']}")

    # client call server
    def setCM(self, req, ctx):
        mprint("*** setCM ***")
        self.failrate = req.vals
        return rnp.setCMResponse(vals=self.failrate)

    # server call server
    def requestVote(self, req, ctx):
        if req.CMReq and random.random() < self.failrate[req.srvId]:
            time.sleep(self.networkTimeout())
            return rnp.requestVoteResponse(code=rnp.CM_FAIL)
        mprint("recv req vote id=", req.candidateId, " term=",
               req.term, "myterm = ", self.currentTerm, self.state, req.term == self.currentTerm, self.votedFor != self.id or self.state == State.LEADER)
        if req.term >= self.currentTerm:
            if req.term == self.currentTerm and (self.votedFor != self.id or self.state == State.LEADER):
                mprint("I am ", self.id, "and reject vote")
                self.writeData()
                return rnp.requestVoteResponse(term=self.currentTerm, code=rnp.REJECT)
            self.currentTerm = req.term
            if self.log[-1].term > req.lastLogTerm or \
                    (self.log[-1].term == req.lastLogTerm and len(self.log) - 1 > req.lastLogIndex):
                mprint("I am ", self.id, "and reject vote")
                self.writeData()
                return rnp.requestVoteResponse(term=self.currentTerm, code=rnp.REJECT)
            self.votedFor = req.candidateId
            self.changeToFollower()
            mprint("change to follower I am ", self.id, "and vote to ",
                   self.votedFor, "myterm", self.currentTerm)
            self.writeData()
            return rnp.requestVoteResponse(term=self.currentTerm, code=rnp.GRANT)
        mprint("I am ", self.id, "and reject vote")
        self.writeData()
        return rnp.requestVoteResponse(term=self.currentTerm, code=rnp.REJECT)

    # server call server
    # replicate log and heartbeat
    def appendEntries(self, req, ctx):
        if req.CMReq and random.random() < self.failrate[req.srvId]:
            time.sleep(self.networkTimeout())
            return rnp.appendEntriesResponse(code=rnp.CM_FAIL)
        mprint('.', req.term, end='')
        sys.stdout.flush()
        self.lastAppendEntriesTime = time.time()
        if req.term >= self.currentTerm:
            if req.term > self.currentTerm:
                self.changeToFollower(req.term)
                mprint("change to follwer, my term ", self.currentTerm)
            self.leaderId = req.leaderId
            if not(len(self.log) > req.prevLogIndex and self.log[req.prevLogIndex].term == req.prevLogTerm):
                self.deleteLog(range(req.prevLogIndex, len(self.log)))
                self.log = self.log[:req.prevLogIndex]
                self.writeData()
                return rnp.appendEntriesResponse(term=self.currentTerm, code=rnp.LOG_INCONSISTENT)

            self.log = self.log[:req.prevLogIndex+1] + \
                [LogEntry(x.term, x.key, x.value, x.uuid) for x in req.entries]
            for i, x in enumerate(self.log[req.prevLogIndex+1:]):
                self.logSet[x.uuid] = i
            self.writeLog(range(req.prevLogIndex+1, len(self.log)))
            self.commitIndex = max(self.commitIndex, req.leaderCommit)
            self.writeData()
            return rnp.appendEntriesResponse(term=self.currentTerm, code=rnp.SUCCESS)
        mprint("(appendEntries) reject")
        self.writeData()
        return rnp.appendEntriesResponse(term=self.currentTerm, code=rnp.REJECT)

    def getStatus(self, req, ctx):
        entries = [rnp.Entry(term=x.term, key=x.key, value=x.value)
                   for x in self.log]
        return rnp.getStatusResponse(term=self.currentTerm,
                                     votedFor=self.votedFor,
                                     entries=entries,
                                     state=self.state.name,
                                     id=self.id,
                                     commitIndex=self.commitIndex,
                                     lastApplied=self.lastApplied,
                                     nextIndex=self.nextIndex,
                                     matchIndex=self.matchIndex)

    def getTimestamp(self, req, ctx):
        timestamps = []
        for fname in os.listdir(self.config["data_path"]):
            tp, term, t = fname.split('-')
            timestamps.append(rnp.Timestamp(
                term=int(term), time=t, type=int(tp)))
        return rnp.getTimestampResponse(timestamps=timestamps)

    def noop(self, req, ctx):
        return rnp.Null()

    def getAppendEntryLatency(self, req, ctx):
        srvId = random.choice(list(set(range(self.srvCnt)) - {self.id}))
        with self.getChannel(srvId) as channel:
            stub = rnpg.raftnodeStub(channel)
            start = time.time()
            resp = stub.appendEntries(rnp.appendEntriesRequest(term=self.currentTerm,
                                                               leaderId=self.id,
                                                               prevLogIndex=self.nextIndex[srvId]-1,
                                                               prevLogTerm=self.log[self.nextIndex[srvId]-1].term,
                                                               entries=req.entries,
                                                               leaderCommit=self.commitIndex,
                                                               CMReq=True,
                                                               srvId=self.id), timeout=0.1)
        return rnp.getAppendEntryLatencyResponse(time=str(time.time() - start))

    def updateConfig(self, req, ctx):
        config = req.config
        try:
            config = json.loads(config)
            json.dump(config, open(
                'config.json', 'w'), sort_keys=True, indent=4)
            return rnp.updateConfigResponse(code=rnp.SUCCESS)
        except Exception as e:
            mprint(e)
            return rnp.updateConfigResponse(code=rnp.FAILURE)

    def writeLog(self, logIndexes):
        try:
            for i in logIndexes:
                with open(self.logIndexFile(i), "wb+") as f:
                    pickle.dump(self.log[i], f)
        except Exception as e:
            mprint("WL Error: ", e)

    def deleteLog(self, logIndexes):
        try:
            for i in logIndexes:
                del self.logSet[pickle.load(
                    open(os.path.join(self.config['log_path'], 'log', str(i)), 'rb')).uuid]
                os.remove(os.path.join(
                    self.config['log_path'], 'log', str(i)))
        except Exception as e:
            mprint("DL Error: ", e)

    def writeData(self):
        try:
            # write currentTerm, votedFor, logEntries
            for x in ['currentTerm', 'votedFor']:
                with open(os.path.join(self.config['log_path'], x), "wb+") as f:
                    pickle.dump(self.__getattribute__(x), f)
        except Exception as e:
            mprint("WD Error: ", e)

    def readLog(self, logIndexes):
        try:
            res = []
            for i in logIndexes:
                with open(self.logIndexFile(i), "rb") as f:
                    res.append(pickle.load(f))
                    self.logSet[res[-1].uuid] = i

            return res
        except Exception as e:
            mprint("RL Error: ", e)

    def readAllLog(self):
        try:
            i = 1
            while os.path.exists(self.logIndexFile(i)):
                i += 1
            return self.readLog(range(1, i))
        except Exception as e:
            mprint("RAL Error: ", e)

    def readData(self):
        try:
            # write currentTerm, votedFor, logEntries
            res = {}
            for x in ['currentTerm', 'votedFor']:
                with open(os.path.join(self.config['log_path'], x), "rb") as f:
                    res[x] = pickle.load(f)
            return res
        except Exception as e:
            mprint("RD Error: ", e)
