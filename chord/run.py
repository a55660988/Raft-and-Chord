import subprocess as sp
import sys

for port in range(int(sys.argv[1]), int(sys.argv[2])+1):
    sp.Popen(['python3', 'server.py', str(port), 'join'],
             stdout=open(f"{port}.log", 'w'))
