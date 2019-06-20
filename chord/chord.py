import chord_pb2 as cp
import chord_pb2_grpc as cpg
from utils import get_public_ip, gen_hash, in_range
import grpc
from threading import Thread
import time
import json
import sys
import uuid
VERBOSE = True


def mprint(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs)
        sys.stdout.flush()


class Chord:
    def __init__(self, port, config):
        self.port = port
        self.config = config
        self.hash = gen_hash(self.config['mbits'])
        self.addr = f"{get_public_ip()}:{self.port}"
        self.leave = False
        self.initialize()
        self.periodic_jobs = Thread(target=self.Periodic)
        self.periodic_jobs.start()
        # Thread(target=self.status).start()

    def initialize(self):
        self.leave = False
        self.store = {}
        self.predecessor = None
        self.hashed_predecessor = None
        self.successor = self.addr
        self.successor_list = [self.addr]+[''] * \
            (self.config['successor_list_len']-1)
        self.hashed_successor = self.hash(self.successor)
        self.hashed_addr = self.hash(self.addr)
        self.next_fingers = 0
        self.fingers = [None] * (self.config['mbits'] + 1)
        self.replica_uuids = set()
        mprint('id', self.hashed_addr)

    def Own(self, id):
        return in_range(self.hashed_predecessor, id,
                        self.hashed_addr) or id == self.hashed_addr

    def status(self):
        time.sleep(5)
        while True:
            mprint('suc', self.successor)
            mprint('pre', self.predecessor)
            mprint('fig', self.fingers)
            mprint('sucl', self.successor_list)
            time.sleep(5)

    def getChannel(self, addr):
        return grpc.insecure_channel(addr)

    def Noop(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        return cp.Null()

    def Reset(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        self.store = {}
        return cp.Null()

    def ClosestPrecedingNode(self, id):
        res = []
        for i in range(self.config['mbits'], 0, -1):
            if self.fingers[i] and in_range(
                    self.hashed_addr, self.fingers[i]['hashed_addr'], id):
                if self.fingers[i]['addr'] not in res:
                    res.append(self.fingers[i]['addr'])
        for x in self.successor_list:
            if x and x not in res:
                res.append(x)
        return res

    def FindSuccessor(self, id):
        successor = None
        plen = 0
        if in_range(self.hashed_addr, id,
                    self.hashed_successor) or id == self.hashed_successor:
            successor = self.successor
        else:
            nxt_addrs = self.ClosestPrecedingNode(id)
            mprint('nxt addr', id, nxt_addrs)
            while len(nxt_addrs):
                nxt_addr = nxt_addrs[0]
                nxt_addrs = nxt_addrs[1:]
                try:
                    with grpc.insecure_channel(nxt_addr) as channel:
                        stub = cpg.chordStub(channel)
                        res = stub.GetSuccessor(
                            cp.GetSuccessorRequest(id=str(id)),
                            timeout=self.config['rpc_timeout'])
                        successor = res.server_addr
                        plen = res.path_length + 1
                    break
                except Exception as e:
                    mprint("fs", e)
        # mprint('get suc', successor)
        return successor, plen

    def GetSuccessor(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        id = int(req.id)
        mprint('get suc', self.hashed_addr, id, self.hashed_successor)
        successor, plen = self.FindSuccessor(id)
        return cp.GetSuccessorResponse(server_addr=successor,
                                       path_length=plen,
                                       code=cp.Success)

    def GetSuccessorList(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        return cp.GetSuccessorListResponse(
            server_addrs=[x if x else '' for x in self.successor_list])

    def Put(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        key, val, id = req.key, req.val, int(req.id)
        if self.hashed_predecessor and (in_range(self.hashed_predecessor, id,
                                                 self.hashed_addr)
                                        or id == self.hashed_addr):
            self.store[key] = {'val': val, 'id': id}
            with grpc.insecure_channel(self.predecessor) as channel:
                stub = cpg.chordStub(channel)
                stub.DirectPut(cp.DirectPutRequest(
                    key=key,
                    id=str(id),
                    val=val,
                    uuid=uuid.uuid4().hex,
                    remaining=self.config['max_failures']),
                               timeout=self.config['rpc_timeout'] *
                               self.config['max_failures'])
            return cp.PutResponse(code=cp.Success)
        else:
            nxt_addr, _ = self.FindSuccessor(id)
            try:
                with grpc.insecure_channel(nxt_addr) as channel:
                    stub = cpg.chordStub(channel)
                    return stub.Put(cp.PutRequest(key=req.key,
                                                  val=req.val,
                                                  id=str(req.id)),
                                    timeout=self.config['rpc_timeout'])
            except Exception as e:
                mprint('put', e)
        return cp.PutResponse(code=cp.Error)

    def DirectPut(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        key, id, val, remaining, uid = req.key, int(
            req.id), req.val, req.remaining, req.uuid
        if uid in self.replica_uuids:
            return cp.DirectPutResponse(code=cp.Success)
        self.replica_uuids.add(uid)

        self.store[key] = {'val': val, 'id': id}
        if remaining > 1:
            with grpc.insecure_channel(self.predecessor) as channel:
                stub = cpg.chordStub(channel)
                stub.DirectPut(cp.DirectPutRequest(key=key,
                                                   id=str(id),
                                                   val=val,
                                                   uuid=uid,
                                                   remaining=remaining - 1),
                               timeout=self.config['rpc_timeout'] * remaining)
        self.replica_uuids.remove(uid)
        return cp.DirectPutResponse(code=cp.Success)

    def DirectPutMulti(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        keys, vals, remaining, uid = req.keys, req.vals, req.remaining, req.uuid
        if uid in self.replica_uuids:
            return cp.DirectPutMultiResponse(code=cp.Success)
        self.replica_uuids.add(uid)

        mprint('multiput', keys, vals, remaining, self.predecessor)
        for k, v in zip(keys, vals):
            self.store[k] = {'val': v, 'id': int(self.hash(k))}
        if remaining > 1:
            try:
                with grpc.insecure_channel(self.predecessor) as channel:
                    stub = cpg.chordStub(channel)
                    stub.DirectPutMulti(
                        cp.DirectPutMultiRequest(keys=keys,
                                                 vals=vals,
                                                 uuid=uid,
                                                 remaining=remaining - 1),
                        timeout=self.config['rpc_timeout'] * remaining)
            except Exception as e:
                mprint('multiputE', e, self.predecessor)
        self.replica_uuids.remove(uid)
        return cp.DirectPutMultiResponse(code=cp.Success)

    def Delete(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        mprint('del', req.key, type(req.id))
        key, id = req.key, int(req.id)
        if self.hashed_predecessor and (in_range(self.hashed_predecessor, id,
                                                 self.hashed_addr)
                                        or id == self.hashed_addr):
            if key in self.store:
                del self.store[key]
                return cp.DeleteResponse(code=cp.Success)
            else:
                return cp.DeleteResponse(code=cp.KEY_NOT_EXIST)
        else:
            nxt_addr, _ = self.FindSuccessor(id)
            try:
                with grpc.insecure_channel(nxt_addr) as channel:
                    stub = cpg.chordStub(channel)
                    return stub.Delete(cp.DeleteRequest(key=req.key,
                                                        id=str(req.id)),
                                       timeout=self.config['rpc_timeout'])
            except Exception as e:
                mprint('get', e)
        return cp.DeleteResponse(code=cp.Error)

    def Get(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        mprint('get', req.key, type(req.id))
        key, id = req.key, int(req.id)
        if self.hashed_predecessor and (in_range(self.hashed_predecessor, id,
                                                 self.hashed_addr)
                                        or id == self.hashed_addr):
            if key in self.store:
                return cp.GetResponse(val=self.store[key]['val'],
                                      server_addr=self.addr,
                                      code=cp.Success)
            else:
                return cp.GetResponse(server_addr=self.addr,
                                      code=cp.KEY_NOT_EXIST)
        else:
            nxt_addr, _ = self.FindSuccessor(id)
            try:
                with grpc.insecure_channel(nxt_addr) as channel:
                    stub = cpg.chordStub(channel)
                    return stub.Get(cp.GetRequest(key=req.key, id=str(req.id)),
                                    timeout=self.config['rpc_timeout'])
            except Exception as e:
                mprint('get', e)
        return cp.GetResponse(code=cp.Error)

    def GetReplicas(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        remaining,uid = req.remaining,req.uuid
        keys, vals = [], []
        if uid in self.replica_uuids:
            return cp.GetReplicasResponse(keys=keys, vals=vals)
        self.replica_uuids.add(uid)

        for k, v in self.store.items():
            if self.Own(self.hash(k)):
                keys.append(k)
                vals.append(v['val'])
        if remaining > 1:
            with grpc.insecure_channel(self.successor) as channel:
                stub = cpg.chordStub(channel)
                res = stub.GetReplicas(
                    cp.GetReplicasRequest(remaining=remaining - 1, uuid=uid),
                    timeout=self.config['rpc_timeout'] * remaining)
                keys += res.keys
                vals += res.vals
        self.replica_uuids.remove(uid)
        return cp.GetReplicasResponse(keys=keys, vals=vals)

    def GetRange(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        from_id, to_id = int(req.from_id), int(req.to_id)
        keys, vals = [], []
        for k in self.store.keys():
            id = self.store[k]['id']
            if id == from_id or id == to_id or in_range(from_id, id, to_id):
                keys.append(k)
                vals.append(self.store[k]['val'])
        return cp.GetRangeResponse(keys=keys, vals=vals)

    def DeleteRange(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        from_id, to_id = int(req.from_id), int(req.to_id)
        for k in list(self.store.keys()):
            id = self.store[k]['id']
            if id == from_id or id == to_id or in_range(from_id, id, to_id):
                del self.store[k]
        return cp.DeleteRangeResponse()

    def Join(self, server_addr):
        self.initialize()
        mprint('join start')
        self.predecessor = None
        self.hashed_predecessor = None
        with grpc.insecure_channel(server_addr) as channel:
            stub = cpg.chordStub(channel)
            res = stub.GetSuccessor(
                cp.GetSuccessorRequest(id=str(self.hashed_addr)),
                timeout=self.config['rpc_timeout'])
            successor = res.server_addr
            successor_list = [successor]+[''] * \
                (self.config['successor_list_len']-1)
        with grpc.insecure_channel(successor) as channel:
            stub = cpg.chordStub(channel)
            # get my own data
            res = stub.GetPredecessor(cp.Null())
            hashed_predecessor = (self.hash(res.server_addr) + 1) & (
                (1 << self.config['mbits']) - 1)
            res = stub.GetRange(
                cp.GetRangeRequest(from_id=str(hashed_predecessor),
                                   to_id=str(self.hashed_addr)))
            for k, v in zip(res.keys, res.vals):
                self.store[k] = {'val': v, 'id': self.hash(k)}
            self.successor = successor
            self.successor_list = successor_list
            # delete successor's my own data
            res = stub.DeleteRange(
                cp.DeleteRangeRequest(from_id=str(hashed_predecessor),
                                      to_id=str(self.hashed_addr)))
            # res = stub.GetReplicas(cp.GetReplicasRequest(
            #     remaining=self.config['max_failures']))
            # for k, v in zip(res.keys, res.vals):
            #     self.store[k] = {'val': v, 'id': self.hash(k), 'own': False}
        mprint('join', self.successor)

    def Leave(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        self.leave = True
        return cp.Null()

    def Resume(self, req, ctx):
        server_addr = req.server_addr
        self.Join(server_addr)
        return cp.ResumeResponse(code=cp.Success)

    def _Notify(self, addr):
        hashed_addr = self.hash(addr)
        # mprint("not", addr)
        if self.predecessor == None or in_range(self.hashed_predecessor,
                                                hashed_addr, self.hashed_addr):
            self.predecessor = addr
            self.hashed_predecessor = hashed_addr
            with grpc.insecure_channel(self.predecessor) as channel:
                stub = cpg.chordStub(channel)
                res = stub.GetRange(
                    cp.GetRangeRequest(
                        from_id=str((self.hashed_predecessor + 1)
                                    & ((1 << self.config['mbits']) - 1)),
                        to_id=str(self.hashed_addr)))
                for k, v in zip(res.keys, res.vals):
                    self.store[k] = {'val': v, 'id': self.hash(k)}
            self._DoReplicate(self.config['max_failures'], uuid.uuid4().hex)

    def Notify(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        self._Notify(req.server_addr)
        return cp.NotifyResponse(code=cp.Success)

    def GetFingers(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        return cp.GetFingersResponse(
            server_addrs=[x['addr'] if x else '' for x in self.fingers])
        pass

    def GetPredecessor(self, req, ctx):
        if self.leave:
            raise Exception('Leave')
            return
        return cp.GetPredecessorResponse(server_addr=self.predecessor)

    def Periodic(self):
        time.sleep(2)
        while True:
            if not self.leave:
                self.Stabilize()
                self.FixFingers()
                self.CheckPredecessor()
            time.sleep(self.config['timeout'])

    def Stabilize(self):
        mprint('stable')
        try:
            x = None
            while self.successor_list[0]:
                self.successor = self.successor_list[0]
                self.hashed_successor = self.hash(self.successor)
                mprint('stable', self.successor)
                try:
                    if self.addr != self.successor:
                        with grpc.insecure_channel(self.successor) as channel:
                            stub = cpg.chordStub(channel)
                            res = stub.GetPredecessor(
                                cp.Null(), timeout=self.config['rpc_timeout'])
                            mprint('pred', self.successor, res)
                            x = res.server_addr
                    else:
                        x = self.predecessor
                    break
                except Exception as e:
                    self.successor_list = self.successor_list[1:] + [None]
                    mprint('s1', e, self.successor_list)

            if x:
                hashed_x = self.hash(x)
                if in_range(self.hashed_addr, hashed_x, self.hashed_successor):
                    self.successor = x
                    self.hashed_successor = self.hash(self.successor)
                    self.successor_list = [self.successor
                                           ] + self.successor_list[:-1]
                    print('update sl', self.successor_list)
            self.successor = self.successor_list[0]
            if self.addr != self.successor:
                with grpc.insecure_channel(self.successor) as channel:
                    stub = cpg.chordStub(channel)
                    res = stub.Notify(cp.NotifyRequest(server_addr=self.addr),
                                      timeout=self.config['rpc_timeout'])
                    res = stub.GetSuccessorList(
                        cp.Null(), timeout=self.config['rpc_timeout'])
                    self.successor_list = [self.successor
                                           ] + res.server_addrs[:-1]
            else:
                self._Notify(self.addr)
        except Exception as e:
            import traceback as tb
            mprint('s2', self.successor_list, e, tb.format_stack())
            pass

    def FixFingers(self):
        try:
            self.next_fingers += 1
            if self.next_fingers > self.config['mbits']:
                self.next_fingers = 1
            mprint('updatefig', self.next_fingers)
            new_finger = {}
            new_finger['addr'], _ = self.FindSuccessor(
                (self.hashed_addr + (1 << (self.next_fingers - 1)))
                & ((1 << self.config['mbits']) - 1))
            if not new_finger['addr']:
                return
            new_finger['hashed_addr'] = self.hash(new_finger['addr'])
            self.fingers[self.next_fingers] = new_finger
            mprint('updatefig', self.next_fingers, new_finger)
        except Exception as e:
            mprint('fix', e)

    def CheckPredecessor(self):
        try:
            mprint('checkpred', self.predecessor)
            if self.predecessor != None:
                mprint(self.predecessor)
                with grpc.insecure_channel(self.predecessor) as channel:
                    stub = cpg.chordStub(channel)
                    stub.Noop(cp.Null(), timeout=self.config['rpc_timeout'])
        except Exception as e:
            mprint('predE', e)
            self.predecessor = None
            self.hashed_predecessor = None

    def UploadConfig(self, req, ctx):
        try:
            config = req.config
            config = json.loads(config)
            json.dump(open('./config.json', 'w'), config)
            return cp.UploadConfigResponse(code=cp.Success)
        except:
            return cp.UploadConfigResponse(code=cp.Error)

    def GetStatus(self, req, ctx):
        keys = list(self.store.keys())
        return cp.GetStatusResponse(
            addr=self.addr,
            successors=[x if x else '' for x in self.successor_list],
            fingers=[x['addr'] if x else '' for x in self.fingers],
            predecessor=self.predecessor,
            next_fingers=self.next_fingers,
            keys=keys,
            vals=[self.store[k]['val'] for k in keys])

    def DoReplicate(self, req, ctx):
        remaining, uid = req.remaining, req.uuid
        if uid in self.replica_uuids:
            return cp.DoReplicateResponse()
        self.replica_uuids.add(uid)

        self._DoReplicate(remaining, uid)
        self.replica_uuids.remove(uid)
        return cp.DoReplicateResponse()

    def _DoReplicate(self, remaining, uid):
        keys = [k for k in self.store if self.Own(self.hash(k))]
        vals = [self.store[k]['val'] for k in keys]
        mprint('dorep', self.predecessor, keys, vals, self.store)
        with grpc.insecure_channel(self.predecessor) as channel:
            stub = cpg.chordStub(channel)
            stub.DirectPutMulti(cp.DirectPutMultiRequest(
                keys=keys, vals=vals, remaining=self.config['max_failures'], uuid=uuid.uuid4().hex),
                                timeout=self.config['rpc_timeout'] *
                                self.config['max_failures'])

        if remaining > 1:
            for addr in list(self.successor_list):
                try:
                    with grpc.insecure_channel(addr) as channel:
                        stub = cpg.chordStub(channel)
                        stub.DoReplicate(
                            cp.DoReplicateRequest(remaining=remaining - 1, uuid=uid),
                            timeout=remaining * self.config['rpc_timeout'])
                    mprint('rep to', addr)
                    break
                except Exception as e:
                    mprint('dor', e)
    def UpdateReplica(self, req, ctx):
        max_failures = req.max_failures
        self.config['max_failures'] = max_failures
        return cp.UpdateReplicaResponse()