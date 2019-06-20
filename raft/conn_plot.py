import pandas as pd
import pandas
from matplotlib import pyplot as plt
import seaborn as sns
import numpy as np


r = open('results_2', 'r').readlines()
r = [x.strip() for x in r if x.strip()]
r = [x.split() for x in r]
df = pd.DataFrame(
    r, columns=['type', 'cm', 'server_cnt', 'client_cnt', 'perf'])
df['cm'] = df['cm'].astype('float64')
df['perf'] = df['perf'].astype('float64') * 1000
df['server_cnt'] = df['server_cnt'].astype('int')
df['client_cnt'] = df['client_cnt'].astype('int')
df = df.drop_duplicates(
    ['type', 'cm', 'server_cnt', 'client_cnt'], keep='last')

title = {
    'get_with_retry': 'Get RPC Performance',
    'put_with_retry': 'Put RPC Performance',
    'get_put_with_retry': 'Mixed Get and Put RPC Performance',
}


def client_perf(t):
    plt.clf()
    sns.set_style('whitegrid')
    plt.title(title[t])
    plt.figure(figsize=(12, 6))
    plt.xlabel('# of clients')
    plt.ylabel('ms / req')
    xticks = np.arange(4, 65, 4)
    plt.xticks(xticks)
    sdf = df[df['type'] == t]
    for i in [0, 0.05, 0.1, 0.25]:
        y = sdf[(sdf['cm'] == i) & (sdf['server_cnt'] == 10)].sort_values(
            'client_cnt')['perf'].values
        sns.lineplot(x=xticks[:len(y)], y=y, label=f"chaos = {i}")

    plt.savefig(t+'_client_perf')


def server_perf(t):
    plt.clf()
    sns.set_style('whitegrid')
    plt.title(title[t])
    plt.figure(figsize=(12, 6))
    plt.xlabel('# of server')
    plt.ylabel('ms / req')
    xticks = np.arange(5, 41, 5)
    plt.xticks(xticks)
    sdf = df[df['type'] == t]
    for i in [0, 0.05, 0.1, 0.25]:
        y = sdf[(sdf['cm'] == i) & (sdf['client_cnt'] == 16)].sort_values(
            'server_cnt')['perf'].values
        sns.lineplot(x=xticks[:len(y)], y=y, label=f"chaos = {i}")

    plt.savefig(t+'_server_perf')


client_perf('get_with_retry')
client_perf('put_with_retry')
client_perf('get_put_with_retry')

server_perf('get_with_retry')
server_perf('put_with_retry')
server_perf('get_put_with_retry')
