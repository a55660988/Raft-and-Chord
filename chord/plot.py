from matplotlib import pyplot as plt
import seaborn as sns
import pickle
from collections import Counter
numK = 1000


def plen():
    plens = []
    for i in range(1, 11):
        plen, _ = pickle.load(open(f'stats_5_{i}_{numK}.pkl', 'rb'))
        print(Counter(plen))
    plt.show()


def kdist():
    for i in range(1, 11):
        _, kd = pickle.load(open(f'stats_5_{i}_{numK}.pkl', 'rb'))
        it = list(map(lambda x: len(x[1]), kd.items()))
        print(sum(it), len(it))


if __name__ == "__main__":
    kdist()
