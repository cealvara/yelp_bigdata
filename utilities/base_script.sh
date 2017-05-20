#! /bin/bash
sudo apt-get update
sudo apt-get install sqlite3
sudo apt-get install -y mpich
sudo apt-get install -y python-pip
sudo apt-get install -y python-dev
sudo pip install mpi4py
sudo apt-get install -y python3-dev
sudo apt-get install -y python3-pip
sudo pip3 install mpi4py
sudo pip install numpy
sudo pip3 install numpy
sudo pip3 install --upgrade google-cloud-storage
sudo touch success.txt
