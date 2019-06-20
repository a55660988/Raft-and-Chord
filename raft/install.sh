#!/bin/bash
sudo apt-get update 
sudo apt-get upgrade -y
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install python3.7 -y
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 1
sudo update-alternatives --config python
sudo apt-get install python3.7-gdbm python3.7-dev python3-pip -y
sudo pip3 install -r requirements.txt
