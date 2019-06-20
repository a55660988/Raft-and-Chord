import raftnode_pb2 as rnp
import raftnode_pb2_grpc as rnpg
import threading
import grpc
import json
import time
from concurrent import futures
import sys

from raftnode import RaftNode


ONE_DAY_IN_SEC = 60 * 60 * 24


def startServer():
    config = json.load(open('./config.json'))
    port = int(sys.argv[1])
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))

    rf = RaftNode(config, port)
    rf.server = server
    rnpg.add_raftnodeServicer_to_server(rf, server)

    server.add_insecure_port(f'[::]:{port}')
    server.start()

    print('setup timeouts and thread runners')
    rf.lastAppendEntriesTime = time.time()
    if rf.id != -1:
        threading.Thread(target=rf.doFollower).start()
        threading.Thread(target=rf.doCandidate).start()
        threading.Thread(target=rf.doLeader).start()

    try:
        while True:
            time.sleep(ONE_DAY_IN_SEC)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    startServer()
