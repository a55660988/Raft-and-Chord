import json
import sys

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python3 genConfig.py ip1 ip2 ip3 ...')
        exit(1)
    config = json.load(open('./config.json'))
    config["servers"] = [{
        "ip": ip,
        "port": 50000 + p
    } for ip in sys.argv[1:] for p in range(5)]
    json.dump(config, open(
        'config.json', 'w'), sort_keys=True, indent=4)
