import grpc
import json
import sys
import raftnode_pb2 as rnp
import raftnode_pb2_grpc as rnpg
import time
import uuid
import random
from multiprocessing import Pool
from threading import Thread


def formatServerIP(srv):
    return f"{srv['ip']}:{srv['port']}"


class Client():

    def __init__(self, _config):
        self.config = _config
        self.setupStubs()

    def setupStubs(self):
        self.srvCnt = len(self.config['servers'])
        self.servers = {}
        self.channels = {}
        for srv in self.config['servers']:
            addr = f"{srv['ip']}:{srv['port']}"
            self.channels[addr] = grpc.insecure_channel(addr)
            self.servers[addr] = rnpg.raftnodeStub(self.channels[addr])

    def close(self):
        for srv in self.channels:
            self.channels[srv].close()

    def checkLeader(self, serverIP):

        response = self.servers[serverIP].isLeader(
            rnp.isLeaderRequest(ip=serverIP))
        return response.isLeader

    def putValue(self, serverIP, key, value, u=None, timeout=1):
        u = u or uuid.uuid4().hex
        response = self.servers[serverIP].putValue(
            rnp.putValueRequest(key=key, value=value, uuid=u), timeout=timeout)
        return response

    def putValueWithRetry(self, serverIP, key, value, timeout=1):
        u = uuid.uuid4().hex
        while True:
            try:
                response = self.servers[serverIP].putValue(
                    rnp.putValueRequest(key=key, value=value, uuid=u), timeout=timeout)
            except:
                continue
            if response.ip != "":
                serverIP = response.ip
            elif response.code == rnp.SUCCESS:
                break
            # else:
            #     print('no leader found before')
            #     break
        return response

    def getValue(self, serverIP, key, timeout=1):
        response = self.servers[serverIP].getValue(
            rnp.getValueRequest(key=key), timeout=timeout)
        return response

    def getValueWithRetry(self, serverIP, key, timeout=1):
        while True:
            try:
                response = self.getValue(serverIP, key)
            except:
                continue
            if response.ip != "":
                serverIP = response.ip
            elif response.code == rnp.SUCCESS or response.code == rnp.KEY_NOT_EXIST:
                break
            # else:
            #     print('no leader found before')
            #     break
        return response

    def getStatus(self, serverIP):
        return self.servers[serverIP].getStatus(rnp.Null())

    def resetServer(self, serverIP):
        while True:
            try:
                return self.servers[serverIP].resetServer(rnp.Null())
            except:
                continue

    def restartServer(self, serverIP, flag=False):
        return self.servers[serverIP].restartServer(rnp.restartServerRequest(flag=flag))

    def noop(self, serverIP, timeout=1):
        return self.servers[serverIP].noop(rnp.Null(), timeout=timeout)

    def updateConfig(self, serverIP, config):
        while True:
            try:
                return self.servers[serverIP].updateConfig(rnp.updateConfigRequest(config=json.dumps(config)))
            except:
                continue

    def getLeader(self):
        server = random.choice(list(self.servers.values()))
        return server.getLeader(rnp.Null())

    def getTimestamp(self):
        timestamps = []
        for _, server in self.servers.items():
            resp = server.getTimestamp(rnp.Null())
            timestamps += list(map(lambda x: (x.term, float(x.time), x.type),
                                   resp.timestamps))
        timestamps.sort()
        return timestamps

    def updateRow(self, serverIP, vals):
        response = self.servers[serverIP].setCM(rnp.setCMRequest(vals=vals))
        return response

    def updateMatrix(self, matrix):
        for server, vals in zip(self.config["servers"], matrix):
            addr = f"{server['ip']}:{server['port']}"

    def getAppendEntryLatency(self, serverIP, hb=True):
        entries = []
        if not hb:
            # generate random entries
            pass
        return self.servers[serverIP].getAppendEntryLatency(rnp.getAppendEntryLatencyRequest(entries=entries))

    def getAllAlive(self):
        res = {}
        for srv in self.config['servers']:
            addr = formatServerIP(srv)
            try:
                self.noop(addr, timeout=1)
                res[addr] = True
            except:
                res[addr] = False
        return res


class ConcurrentClient:
    def __init__(self, config, np, nt, func):
        self.np = np
        self.nt = nt
        self.config = config
        self.func = func

    def run_process(self, arg):
        self.client = Client(self.config)
        start = time.time()
        tid = [None]*self.nt
        for i in range(self.nt):
            tid[i] = Thread(target=self.func, args=(self,))
            tid[i].start()
        for i in range(self.nt):
            tid[i].join()
        return time.time()-start

    def run(self):
        with Pool(self.np) as p:
            res = p.map(self.run_process, [None]*self.np)
        return sum(res)


def test(self):
    client = self.client
    print(client)
    for i in range(10):

        r = client.putValueWithRetry(formatServerIP(
            self.config['servers'][i % len(self.config['servers'])]), str(i), str(i))
        # r = client.getLeader()
        # print(r)


if __name__ == "__main__":
    config = json.load(open('./config.json', 'r'))
    # cc = ConcurrentClient(config, 2, 2, test)
    # print(cc.run())
    # exit()
    client = Client(config)
    print(client.getAllAlive())
    # exit()
    for srv in config['servers']:
        addr = formatServerIP(srv)
        print('reset', addr)
        client.resetServer(addr)
    for srv in config['servers']:
        print('restart', addr)
        addr = formatServerIP(srv)
        client.updateConfig(addr, config)
        try:
            client.restartServer(addr)
        except:
            pass
    print(client.getAllAlive())
    exit()

    # for i in range(3):
    #     print("Leader is", client.getLeader())

    # for srv in client.servers:
    #     print("srv = ", srv)
    #     client.updateConfig(srv, config)
    #     client.resetServer(srv)
    #     try:
    #         client.restartServer(srv)
    #     except Exception as e:
    #         print(e)

    for srv in client.servers:
        print("srv = ", srv)
        print(client.getStatus(srv))

    # resp = client.putValue(formatServerIP(config['servers'][3]), "key10000", "value10000")
    # if resp.code == rnp.NOT_LEADER and resp.ip != "":
    #     print("leader ip = ", resp.ip)
    # elif resp.code == rnp.SUCCESS:
    #     print("success")
    # else:
    #     print("no leader before")

    # client.updateRow(srv, 0)

    # make two partition group
    # test both group

    # client.close()
    # for srv in client.servers:
    #     client.updateConfig(srv, config)
    #     client.resetServer(srv)
    #     try:
    #         client.restartServer(srv)
    #     except Exception as e:
    #         print(e)
    # exit()
    # client = Client(config)
    # print(client.getLeader(formatServerIP(config['servers'][0])))
    # print(client.putValueWithRetry(formatServerIP(
    #     config['servers'][0]), "key", "value"))
    # print(client.getValueWithRetry(formatServerIP(
    #     config['servers'][0]), "key"))
    # # print(client.getStatus(serverIP))
    # # print(client.updateConfig(serverIP, config))
    # exit()
