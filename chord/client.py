import chord_pb2 as cp
import chord_pb2_grpc as cpg
import grpc
from utils import gen_hash

h = gen_hash(10)

if __name__ == '__main__':
    with grpc.insecure_channel('127.0.0.1:40000') as channel:
        # h9 = (h('127.0.0.1:50001')+(1 << 9)) & ((1 << 10)-1)
        stub = cpg.chordStub(channel)
        # for i in range(10):
        #     stub.Put(cp.PutRequest(key=str(i), val=str(i), id=str(h(str(i)))))
        print(stub.GetStatus(cp.Null()).successor_list)
        # print(stub.Get(cp.GetRequest(key="0", id=str(h("0")))))
        # print(stub.GetSuccessor(cp.GetSuccessorRequest(id=str(h9))))
        exit()
        # stub.Leave(cp.Null())
        # stub.Resume(cp.ResumeRequest(server_addr='127.0.0.1:50003'))
        key = 'key1'
        val = 'val2'
        hk = str(h(key))
        print(key, val, hk)
        res = stub.Put(cp.PutRequest(key=key, val=val, id=str(h(key))))
        res = stub.Get(cp.GetRequest(key=key, id=hk))
        print(res)
        # # res = stub.Noop(cp.Null())
        # # res = stub.GetSuccessor(
        # #     cp.GetSuccessorRequest(id=str(h('127.0.0.1:50001'))))
        # print(res)
