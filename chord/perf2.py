import chord_pb2 as cp
import sys
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
import random

if __name__ == '__main__':
    config = json.load(open('config.json', 'r'))
    h = gen_hash(config['mbits'])
    N = int(sys.argv[1])
    for r in range(1, 26):
        for srv in tqdm(config['servers']):
            with grpc.insecure_channel(srv) as channel:
                stub = cpg.chordStub(channel)
                stub.Reset(cp.Null())
                stub.UpdateReplica(cp.UpdateReplicaRequest(max_failures=r))
        time.sleep(3)
        s = time.time()
        for i in tqdm(range(N)):
            srv = random.choice(config['servers'])
            with grpc.insecure_channel(srv) as channel:
                stub = cpg.chordStub(channel)
                stub.Put(
                    cp.PutRequest(key=str(i), val=str(i), id=str(h(str(i)))))
        t_put = time.time() - s
        s = time.time()
        for i in tqdm(range(N)):
            srv = random.choice(config['servers'])
            with grpc.insecure_channel(srv) as channel:
                stub = cpg.chordStub(channel)
                stub.Get(cp.GetRequest(key=str(i), id=str(h(str(i)))))
        t_get = time.time() - s
        print(t_put, t_get)
        pickle.dump((t_put, t_get), open(f"r2pg_{r}.pkl", 'wb'))
