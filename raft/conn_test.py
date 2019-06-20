from client import Client
from client import ConcurrentClient
from client import formatServerIP
import json
import random
import copy
import uuid
import time

config = json.load(open('./config.json', 'r'))


def generateNewConfig(servers):
    new_config = copy.deepcopy(config)
    new_config['servers'] = servers
    return new_config


def run_get(self):
    client = self.client
    config = self.config
    for _ in range(1000//(self.np*self.nt)):
        addr = formatServerIP(random.choice(config['servers']))
        try:
            client.getValue(addr, "key")
        except:
            pass


def run_get_with_retry(self):
    client = self.client
    config = self.config
    for _ in range(1000//(self.np*self.nt)):
        addr = formatServerIP(random.choice(config['servers']))
        client.getValueWithRetry(addr, "key")


def run_put(self):
    client = self.client
    config = self.config
    for i in range(1000//(self.np*self.nt)):
        addr = formatServerIP(random.choice(config['servers']))
        try:
            client.putValue(addr, str(i), str(i))
        except:
            pass


def run_put_with_retry(self):
    client = self.client
    config = self.config
    for i in range(1000//(self.np*self.nt)):
        addr = formatServerIP(random.choice(config['servers']))
        client.putValueWithRetry(addr, str(i), str(i))


def run_get_put_with_retry(self):
    client = self.client
    config = self.config
    for i in range(1000//(self.np*self.nt)):
        addr = formatServerIP(random.choice(config['servers']))
        if random.random() < 0.5:
            key = uuid.uuid4().hex
            client.putValueWithRetry(addr, str(i), uuid.uuid4().hex)
        else:
            client.getValueWithRetry(addr, str(random.randint(0, i)))


def perf(cm, servers, client_num, func):
    client = Client(config)
    new_config = generateNewConfig(servers)
    empty_config = copy.deepcopy(config)
    empty_config['servers'] = []
    for srv in config['servers']:
        addr = formatServerIP(srv)
        client.resetServer(addr)
    for srv in config['servers']:
        addr = formatServerIP(srv)
        client.updateConfig(addr, empty_config)
    for srv in servers:
        addr = formatServerIP(srv)
        client.updateConfig(addr, new_config)
    for srv in config['servers']:
        addr = formatServerIP(srv)
        try:
            client.restartServer(addr)
        except:
            pass

    for srv in servers:
        addr = formatServerIP(srv)
        while True:
            try:
                client.noop(addr)
                break
            except:
                pass
    client.close()
    client = Client(new_config)
    for srv in servers:
        addr = formatServerIP(srv)
        client.updateRow(addr, [cm]*len(servers))
    client.close()
    nt = 4
    np = client_num//nt
    client = ConcurrentClient(new_config, np, nt, func)
    return client.run()/1000


test_funcs = {
    'get': run_get,
    'put': run_put,
    'get_with_retry': run_get_with_retry,
    'put_with_retry': run_put_with_retry,
    'get_put_with_retry': run_get_put_with_retry
}

if __name__ == '__main__':
    servers = config['servers']
    # for server_num in range(10, 11, 5):
    #     for client_num in range(64, 65, 4):
    #         for cm in [0, 0.05, 0.1, 0.25]:
    #             for k, f in test_funcs.items():
    #                 random.shuffle(servers)
    #                 sub_servers = servers[:server_num]
    #                 print(k, cm, server_num, client_num, perf(
    #                     cm, sub_servers, client_num, f))
    #                 time.sleep(2)

    # for server_num in range(5, 41, 5):
    #     for client_num in range(16, 17, 4):
    #         for cm in [0, 0.05, 0.1, 0.25]:
    #             for k, f in test_funcs.items():
    #                 random.shuffle(servers)
    #                 sub_servers = servers[:server_num]
    #                 print(k, cm, server_num, client_num, perf(
    #                     cm, sub_servers, client_num, f))
    #                 time.sleep(2)

    server_num = 20
    random.shuffle(servers)
    sub_servers = servers[:server_num]
    print('get_with_retry', 0.05, server_num, 16,
          perf(0.05, sub_servers, 16, run_get_with_retry))
