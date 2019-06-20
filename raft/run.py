import subprocess as sp
import sys

s, e = int(sys.argv[1]), int(sys.argv[2])
for i in range(s, e+1):
    sp.Popen(['python3', 'server.py', str(i)], stdout=open(f"{i}.log", "w"))
