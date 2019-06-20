import chord_pb2 as cp
import grpc
import chord_pb2_grpc as cpg
from concurrent import futures
import sys
import time
from chord import Chord
import json
import grpc


if __name__ == '__main__':
    port = int(sys.argv[1])
    t = sys.argv[2]
    config = json.load(open('config.json', 'r'))
    c = Chord(port, config)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    if t == 'join':
        print('join to ', config['master'])
        c.Join(config['master'])
    cpg.add_chordServicer_to_server(c, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()

    try:
        while True:
            time.sleep(60*60*24)
    except KeyboardInterrupt:
        server.stop(0)
