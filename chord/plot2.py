import pickle
import pandas as pd
import pandas
from matplotlib import pyplot as plt
import seaborn as sns
import numpy as np

plt.clf()
sns.set_style('whitegrid')
plt.figure(figsize=(12, 6))
plt.xlabel('# of replicas')
plt.ylabel('ms / req')
xticks = np.arange(1, 26)
plt.xticks(xticks)
ps, gs = [], []
for i in range(1, 26):
    (t_put, t_get) = pickle.load(open(f"r2pg_{i}.pkl", 'rb'))
    ps.append(t_put)
    gs.append(t_get)
sns.lineplot(x=xticks, y=ps, label='Put', marker='o')
sns.lineplot(x=xticks, y=gs, label='Get', marker='o')
plt.savefig('r2pg')
