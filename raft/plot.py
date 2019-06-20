import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns
import pickle


def grpcperf(fname):
    data = pickle.load(open(fname, 'rb'))
    data = data.astype(np.float) * 1000

    keys = ['Noop RTT', 'Put RTT', 'Get RTT', 'Heartbeat RTT']

    f, axes = plt.subplots(2, 2, figsize=(12, 8))

    for i in range(2):
        for j in range(2):
            axes[i][j].set_title(keys[i * 2 + j])
            axes[i][j].set_xlabel('RTT (ms)')
            sns.kdeplot(data[i * 2 + j], ax=axes[i][j])

    plt.setp(axes, yticks=[])
    plt.tight_layout()

    print(f'Mean: {np.mean(data, axis=1)}')
    print(f'Std: {np.std(data, axis=1)}')
    plt.savefig(f'{fname[:-7]}.png')


def leperf(fname):
    sns.set_style('whitegrid')
    data = pickle.load(open(fname, 'rb'))

    fig, ax = plt.subplots(figsize=(10, 6))
    plt.xlim(0, 1500)
    plt.ylim(0, 1)
    plt.xlabel('Time without leader (ms)')
    plt.ylabel('Cumulative Percent')
    for timeout, diffs in data.items():
        diffs = sorted(diffs * 1000)
        sns.lineplot(
            diffs, np.linspace(0, 1, num=len(diffs)), label=f'{int(timeout[0] * 1000)}-{int(timeout[1] * 1000)} ms', ax=ax)

    ax.set_yticklabels([f'{x:.0%}' for x in ax.get_yticks()])
    # plt.show()
    plt.savefig(f'{fname[:-7]}.png')


def stability(fname):
    sns.set_style('whitegrid')
    data = pickle.load(open(fname, 'rb'))

    fig, ax = plt.subplots(figsize=(10, 8))
    plt.xlabel('Timeout (ms)')
    plt.ylabel('# of changes of leader')
    plt.xticks(ticks=range(len(data)), labels=[
               f'{int(round(s * 1000, 0))}-{int(round(t * 1000, 0))} ms' for s, t in data.keys()], rotation=30)

    sns.lineplot(range(len(data)), [len(ts) for ts in data.values()])

    # plt.show()
    plt.savefig(f'{fname[:-7]}.png')


if __name__ == "__main__":
    # grpcperf('./grpcperf_100_4x5.pickle')
    # leperf('leaderEperf_100_150s.pickle')
    stability('stability.pickle')
