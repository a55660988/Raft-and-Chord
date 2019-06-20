import json
import sys
import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--servers', nargs='+',
                        help='List of server ips', required=True)
    parser.add_argument('-r', '--range', type=int,
                        help='Range of ports (50000 ~ 50000 + r)', required=True)
    args = parser.parse_args()

    config = json.load(open('./config.json'))
    config["master"] = f'{args.servers[0]}:50000'
    config["servers"] = [
        f'{ip}:{50000 + p}' for p in range(args.range) for ip in args.servers]
    json.dump(config, open(
        'config.json', 'w'), sort_keys=True, indent=4)
