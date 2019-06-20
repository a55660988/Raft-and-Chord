import json
import chord_pb2 as cp
import chord_pb2_grpc as cpg
import grpc
import subprocess as sp
import time
import unittest
from utils import gen_hash, in_range
import random
from collections import defaultdict


class Client:
    def __init__(self, n, config):
        self.config = config
        self.hash = gen_hash(self.config['mbits'])
        self.n = n
        self.config['master'] = "127.0.0.1:40000"
        self.procs = {}

    def wait_until_alive(self, addr):
        while True:
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = cpg.chordStub(channel)
                    stub.Noop(cp.Null())
                    break
            except:
                time.sleep(1)

    def wait_until_stable(self, addr):
        last = None
        while True:
            res = self.GetPredecessor(addr)
            if last == res.server_addr and res.server_addr:
                break
            last = res.server_addr
            time.sleep(self.config['timeout'] * 3)
        while True:
            res = self.GetSuccessorList(addr)
            if last == res.server_addrs:
                break
            last = res.server_addrs
            time.sleep(self.config['timeout'] * self.config['max_failures'] *
                       3)
        while True:
            res = self.GetFingers(addr)
            if last == res.server_addrs:
                break
            last = res.server_addrs
            time.sleep(self.config['timeout'] * self.config['mbits'] * 2)

    def start(self):
        json.dump(self.config, open("./config.json", "w"))
        self.procs["127.0.0.1:40000"] = sp.Popen(
            ['python3', 'server.py', '40000', 'master'],
            stdout=open('40000.log', 'w'))
        print('waiting for master starts')
        self.wait_until_alive("127.0.0.1:40000")
        for p in range(self.n - 1):
            port = 50000 + p
            self.procs[f"127.0.0.1:{port}"] = sp.Popen(
                ['python3', 'server.py',
                 str(port), 'join'],
                stdout=open(f"{port}.log", "w"))
        print('waiting for stable')
        for p in range(self.n - 1):
            port = 50000 + p
            print(f"127.0.0.1:{port} alive")
            self.wait_until_alive(f"127.0.0.1:{port}")
        for addr in self.procs:
            print(f"{addr} stable")
            self.wait_until_stable(addr)

    def stop(self):
        for _, p in self.procs.items():
            p.kill()

    def GetSuccessorList(self, addr):
        with grpc.insecure_channel(addr) as channel:
            stub = cpg.chordStub(channel)
            res = stub.GetSuccessorList(cp.Null())
        return res

    def GetPredecessor(self, addr):
        with grpc.insecure_channel(addr) as channel:
            stub = cpg.chordStub(channel)
            res = stub.GetPredecessor(cp.Null())
        return res

    def GetFingers(self, addr):
        with grpc.insecure_channel(addr) as channel:
            stub = cpg.chordStub(channel)
            res = stub.GetFingers(cp.Null())
        return res

    def Get(self, addr, key):
        with grpc.insecure_channel(addr) as channel:
            stub = cpg.chordStub(channel)
            res = stub.Get(cp.GetRequest(key=key, id=str(self.hash(key))))
        return res

    def Put(self, addr, key, val):
        with grpc.insecure_channel(addr) as channel:
            stub = cpg.chordStub(channel)
            res = stub.Put(
                cp.PutRequest(key=key, id=str(self.hash(key)), val=val))
        return res

    def Leave(self, addr):
        with grpc.insecure_channel(addr) as channel:
            stub = cpg.chordStub(channel)
            stub.Leave(cp.Null())

    def Resume(self, addr, server_addr):
        with grpc.insecure_channel(addr) as channel:
            stub = cpg.chordStub(channel)
            stub.Resume(cp.ResumeRequest(server_addr=server_addr))

    def GetStatus(self, addr):
        with grpc.insecure_channel(addr) as channel:
            stub = cpg.chordStub(channel)
            res = stub.GetStatus(cp.Null())
        return res


class TestChordRing(unittest.TestCase):
    def setUp(self):
        self.NUM_SERVERS = 5
        self.client = Client(self.NUM_SERVERS,
                             json.load(open('./config.json', 'r')))
        self.hash = self.client.hash
        self.client.start()

    def tearDown(self):
        self.client.stop()
        del self.client
        sp.call(
            "ps aux | grep 'python3 server' | awk '{print $2}' | xargs -I {} kill -9 {}",
            shell=True)

    def test_rings(self):
        sl = {}
        for addr in self.client.procs:
            sl[addr] = self.client.GetSuccessorList(addr).server_addrs
        rings = [list(self.client.procs.keys())[0]]
        print(rings)
        print(sl)
        print(list(self.client.procs.keys()))
        for _ in range(len(self.client.procs) - 1):
            rings.append(sl[rings[-1]][0])
        print(rings)
        # test rings correctness
        self.assertEqual(len(rings), len(self.client.procs))
        self.assertEqual(sorted(rings), sorted(self.client.procs.keys()))
        rings = rings * 3
        for i in range(1, self.client.n + 2):
            self.assertTrue(
                in_range(self.hash(rings[i - 1]), self.hash(rings[i]),
                         self.hash(rings[i + 1])))
        # test successor list correctness
        for addr in self.client.procs:
            self.assertEqual(len(sl[addr]),
                             self.client.config['successor_list_len'])
            for x in sl[addr]:
                self.assertTrue(x in self.client.procs.keys())
            rid = rings.index(addr)
            self.assertEqual(
                sl[addr], rings[rid + 1:rid + 1 +
                                self.client.config['successor_list_len']])
        # test predecessor correctness
        p = {}
        for addr in self.client.procs:
            p[addr] = self.client.GetPredecessor(addr).server_addr
            self.assertTrue(p[addr] in self.client.procs.keys())
            rid = rings.index(addr) + len(self.client.procs.keys())
            self.assertEqual(rings[rid - 1], p[addr])
        # test finger table correctness
        f = {}
        for addr in self.client.procs:
            f[addr] = self.client.GetFingers(addr).server_addrs
            print(addr, f[addr])
            self.assertEqual(len(f[addr]), self.client.config['mbits'] + 1)
            for x in f[addr][1:]:
                self.assertTrue(x in self.client.procs.keys())
            hashed_addr = self.hash(addr)
            for i in range(1, self.client.config['mbits'] + 1):
                f_hashed_addr = (hashed_addr+(1 << (i-1)))\
                    & ((1 << self.client.config['mbits'])-1)
                rid = rings.index(f[addr][i])
                self.assertTrue(
                    in_range(self.hash(rings[rid - 1]), f_hashed_addr,
                             self.hash(rings[rid]))
                    or f_hashed_addr == self.hash(rings[rid]))

    def test_find_successor(self):
        sl = {}
        for addr in self.client.procs:
            sl[addr] = self.client.GetSuccessorList(addr).server_addrs
        rings = [list(self.client.procs.keys())[0]]
        for _ in range(len(self.client.procs) - 1):
            rings.append(sl[rings[-1]][0])
        print(rings)
        for i in range(1000):
            hi = self.hash(str(i))
            n = {
                self.client.Get(addr, str(i)).server_addr
                for addr in self.client.procs
            }
            self.assertEqual(len(n), 1)
            n = list(n)[0]
            hn = self.hash(n)
            p = self.client.GetPredecessor(n).server_addr
            hp = self.hash(p)
            self.assertTrue(in_range(hp, hi, hn) or hn == hi)

    def test_leave_join(self):
        for addr in list(self.client.procs.keys()):
            print(addr, 'Leave')
            self.client.Leave(addr)
            proc = self.client.procs.pop(addr)
            for x in self.client.procs:
                print(x, 'stable')
                self.client.wait_until_stable(x)
            self.test_rings()
            self.test_find_successor()
            self.test_kvstore_replicas()
            self.client.procs[addr] = proc
            print(addr, 'Resume')
            self.client.Resume(addr, list(self.client.procs.keys())[0])
            for x in self.client.procs:
                print(x, 'stable')
                self.client.wait_until_stable(x)
            self.test_rings()
            self.test_find_successor()
            self.test_kvstore_replicas()

    def test_kvstore_replicas(self, put=True):
        if put:
            for i in range(10):
                self.client.Put(random.choice(list(self.client.procs.keys())),
                                str(i), str(i))
        s, p = {}, {}
        for addr in self.client.procs.keys():
            s[addr] = self.client.GetStatus(addr)
            p[addr] = self.client.GetPredecessor(addr).server_addr
        print(s)
        print(p)

        cnt = defaultdict(int)
        for addr in self.client.procs.keys():
            hp = self.hash(p[addr])
            ha = self.hash(addr)
            for k, v in zip(s[addr].keys, s[addr].vals):
                self.assertEqual(k, v)
                cnt[k] += 1
        for k, v in cnt.items():
            self.assertGreaterEqual(v, self.client.config['max_failures'] + 1)

    def test_put_get(self):
        kv = {}
        for _ in range(1000):
            kv[str(random.random())] = str(random.random())
        for k, v in kv.items():
            self.client.Put(random.choice(self.client.procs.keys()), k, v)
        for k, v in kv.items():
            for addr in self.client.procs:
                res = self.client.Get(addr, k)
                self.assertEqual(res.val, v)

    def test_sequential_failure(self):
        kv = {}
        for i in range(100):
            kv[str(i)] = str(i)
            self.client.Put(random.choice(list(self.client.procs.keys())),
                            str(i), str(i))

        self.test_kvstore_replicas(put=False)
        for addr in list(self.client.procs.keys()):
            print(addr, 'Leave')
            self.client.Leave(addr)
            proc = self.client.procs.pop(addr)
            for x in self.client.procs:
                print(x, 'stable')
                self.client.wait_until_stable(x)
            self.test_kvstore_replicas(put=False)
            for k, v in kv.items():
                for x in list(self.client.procs.keys()):
                    self.assertEqual(self.client.Get(x, k).val, v)
            self.client.procs[addr] = proc
            print(addr, 'Resume')
            self.client.Resume(addr, list(self.client.procs.keys())[0])
            for x in self.client.procs:
                print(x, 'stable')
                self.client.wait_until_stable(x)
            self.test_kvstore_replicas(put=False)
            for k, v in kv.items():
                for x in list(self.client.procs.keys()):
                    self.assertEqual(self.client.Get(x, k).val, v)

    def test_all_failures(self):
        kv = {}
        for i in range(100):
            kv[str(i)] = str(i)
            self.client.Put(random.choice(list(self.client.procs.keys())),
                            str(i), str(i))

        self.test_kvstore_replicas(put=False)
        for _ in range(10):
            addrs = list(self.client.procs.keys())
            random.shuffle(addrs)
            addrs = addrs[:self.client.config['max_failures']]
            print(addrs, 'Leave')
            procs = {}
            for addr in addrs:
                self.client.Leave(addr)
                procs[addr] = self.client.procs.pop(addr)
            for x in self.client.procs:
                print(x, 'stable')
                self.client.wait_until_stable(x)
            self.test_kvstore_replicas(put=False)
            for k, v in kv.items():
                for x in list(self.client.procs.keys()):
                    self.assertEqual(self.client.Get(x, k).val, v)
            print(addrs, 'Resume')
            for addr in addrs:
                self.client.procs[addr] = procs[addr]
                self.client.Resume(addr, list(self.client.procs.keys())[0])
            for x in self.client.procs:
                print(x, 'stable')
                self.client.wait_until_stable(x)
            self.test_kvstore_replicas(put=False)
            for k, v in kv.items():
                for x in list(self.client.procs.keys()):
                    self.assertEqual(self.client.Get(x, k).val, v)


if __name__ == '__main__':
    unittest.main(warnings='ignore')
