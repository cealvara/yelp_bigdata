#! /bin/bash
apt-get install -y mpich
apt-get install -y python-pip
apt-get install -y python-dev
pip install mpi4py
apt-get install -y python3-dev
apt-get install -y python3-pip
pip3 install mpi4py
pip install numpy
pip3 install numpy
mkdir /mnt/storage
mount /dev/sdb/ /mnt/storage