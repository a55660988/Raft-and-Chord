import chord_pb2 as cp
import chord_pb2_grpc as cpg
import grpc
import json
import uuid
from tqdm import tqdm
import numpy as np
import argparse
from utils import gen_hash
import pickle
import time


def pl_kd(config, rg, kvs):

    f = gen_hash(config['mbits'])
    plens = []
    for key, val in tqdm(kvs.items()):
        with grpc.insecure_channel(np.random.choice(config['servers'][:rg])) as channel:
            stub = cpg.chordStub(channel)
            resp = stub.Put(cp.PutRequest(
                key=key, val=val, id=str(f(key))))

    for key in tqdm(kvs.keys()):
        with grpc.insecure_channel(np.random.choice(config['servers'][:rg])) as channel:
            stub = cpg.chordStub(channel)
            resp = stub.GetSuccessor(
                cp.GetSuccessorRequest(id=str(f(key))))
            plens.append(resp.path_length)
    plens = np.asarray(plens)

    kd = {}
    for srv in config['servers'][:rg]:
        with grpc.insecure_channel(srv) as channel:
            stub = cpg.chordStub(channel)
            resp = stub.GetStatus(cp.Null())
            kd[srv] = list(resp.keys)

    for srv in config['servers'][:rg]:
        with grpc.insecure_channel(srv) as channel:
            stub = cpg.chordStub(channel)
            resp = stub.Reset(cp.Null())

    return plens, kd


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-n', help='Number of keys to insert', type=int, default=1000)
    return parser.parse_args()


def check_stable(config, rg):
    fgs = {}
    for srv in config['servers'][:rg]:
        with grpc.insecure_channel(srv) as channel:
            stub = cpg.chordStub(channel)
            resp = stub.GetFingers(cp.Null())
            fgs[srv] = resp.server_addrs
    time.sleep(12 * rg)
    for srv in config['servers'][:rg]:
        with grpc.insecure_channel(srv) as channel:
            stub = cpg.chordStub(channel)
            resp = stub.GetFingers(cp.Null())
            if fgs[srv] != resp.server_addrs:
                return False
    return True


if __name__ == "__main__":
    config = json.load(open('config.json', 'r'))
    args = get_args()
    kvs = {}
    for _ in tqdm(range(args.n)):
        rstr = uuid.uuid4().hex
        kvs[rstr[:16]] = rstr[16:]
    while not check_stable(config, 50):
        time.sleep(1)
    for i in tqdm(reversed(range(10))):
        plens, kd = pl_kd(config, 5 * (i + 1), kvs)
        pickle.dump((plens, kd), open(f'stats_5_{i+1}_{args.n}.pkl', 'wb'))
        if i != 0:
            for srv in config['servers'][5 * i:5 * (i + 1)]:
                with grpc.insecure_channel(srv) as channel:
                    stub = cpg.chordStub(channel)
                    stub.Leave(cp.Null())
            while not check_stable(config, 5 * i):
                time.sleep(1)
