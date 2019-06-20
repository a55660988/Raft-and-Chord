
from client import Client
import json
import time
import numpy as np
import math
import uuid
import random
from tqdm import tqdm
import pickle


def getLeaderElection(client, N=1000):
    tN = 0
    while tN < N:
        print(f'{N - tN} resets left...', end='\r')
        try:
            leader = client.getLeader()
            if leader.ip:
                tN += 1
            else:
                time.sleep(1)
                continue
            client.restartServer(leader.ip)
        except:
            alive = False
            while not alive:
                try:
                    client.noop(leader.ip)
                    alive = True
                except:
                    time.sleep(1)
    print()
    timestamps = client.getTimestamp()
    print(f'# of timestamps: {len(timestamps)}')
    diff = []
    idx = 0
    while idx < len(timestamps) - 1 and len(diff) < N:
        if timestamps[idx+1][0] > timestamps[idx][0] and timestamps[idx][2] == 1 and timestamps[idx+1][2] == 0:
            diff.append(timestamps[idx+1][1] - timestamps[idx][1])
        idx += 1

    return np.array(diff)


def leaderElectionPerformance(client, config, trial=100):
    originalET = config["electionTimeout"]
    originalHB = config["heartbeatTimeout"]
    # timeouts = [(round(i, 6), round(i * 2, 6))
    #             for i in [0.05 + 0.025 * j for j in range(9)]]
    # timeouts = [(i, i * 2) for i in [0.012, 0.025, 0.05, 0.1, 0.15]]
    timeouts = [(0.15, i) for i in [0.15, 0.151, 0.155, 0.175, 0.20]]
    diff = {}
    for timeout in timeouts:
        config["electionTimeout"] = timeout
        config["heartbeatTimeout"] = (timeout[0] / 10, timeout[1] / 10)
        for addr in client.servers.keys():
            client.updateConfig(addr, config)
            client.resetServer(addr)
            try:
                client.restartServer(addr, True)
            except:
                pass
        print('Wait 5 seconds for reset...')
        time.sleep(5)
        diff[timeout] = getLeaderElection(client, trial)
    config["electionTimeout"] = originalET
    config["heartbeatTimeout"] = originalHB
    for addr in client.servers.keys():
        client.updateConfig(addr, config)
        client.resetServer(addr)
        try:
            client.restartServer(addr, True)
        except:
            pass

    pickle.dump(diff, open(f'leaderEperf_{trial}.pickle', 'wb+'))


def grpcPerformance(client, trial=100):
    leader = client.getLeader()
    # Null rtt client <-> server
    diff = []
    rtts = []
    keys = []
    for _ in tqdm(range(trial), desc='Noop'):
        s = time.time()
        client.noop(leader.ip)
        rtts.append(time.time() - s)
    diff.append(rtts)
    rtts = []

    # Put rtt client <-> server
    for _ in tqdm(range(trial), desc='Put Rtt'):
        s = time.time()
        keys.append(uuid.uuid4().hex[:8])
        resp = client.putValue(leader.ip, keys[-1], keys[-1][:3])
        rtts.append(time.time() - s)
    diff.append(rtts)
    rtts = []

    # Get rtt client <-> server
    for _ in tqdm(range(trial), desc='Get Rtt'):
        s = time.time()
        resp = client.getValue(leader.ip, random.choice(keys))
        rtts.append(time.time() - s)
    diff.append(rtts)
    rtts = []

    # Get append entry(hb) server <-> server
    for _ in tqdm(range(trial), desc='heartbeat'):
        resp = client.getAppendEntryLatency(leader.ip)
        rtts.append(resp.time)
    diff.append(rtts)
    rtts = []

    pickle.dump(np.array(diff), open(f'grpcperf_{trial}.pickle', 'wb+'))


def stability(client, config):
    originalET = config["electionTimeout"]
    originalHB = config["heartbeatTimeout"]
    timeouts = map(lambda x: (x, x * 2), np.linspace(0.005, 0.08, 16))
    diff = {}
    for timeout in timeouts:
        config["electionTimeout"] = timeout
        config["heartbeatTimeout"] = (timeout[0] / 10, timeout[1] / 10)
        for addr in client.servers.keys():
            client.updateConfig(addr, config)
            client.resetServer(addr)
            try:
                client.restartServer(addr, True)
            except:
                pass
        print('Wait 10 seconds for server stabling...')
        time.sleep(10)

        timestamps = [t for t in client.getTimestamp() if t[2] == 0]
        print(f'# of timestamps: {len(timestamps)}')
        diff[timeout] = np.array(timestamps)

    config["electionTimeout"] = originalET
    config["heartbeatTimeout"] = originalHB
    for addr in client.servers.keys():
        client.updateConfig(addr, config)
        client.resetServer(addr)
        try:
            client.restartServer(addr, True)
        except:
            pass

    pickle.dump(diff, open(f'stability.pickle', 'wb+'))


if __name__ == "__main__":

    config = json.load(open('./config.json'))
    client = Client(config)
    stability(client, config)
