#!/bin/bash

sudo easy_install oauth2
sudo easy_install ipython
sudo easy_install ujson
sudo easy_install pymongo
sudo yum -y install git gcc
sudo yum -y update
echo "[MongoDB]
name=MongoDB Repository
baseurl=http://downloads-distro.mongodb.org/repo/redhat/os/x86_64
gpgcheck=0
enabled=1" | sudo tee -a /etc/yum.repos.d/mongodb.repo
sudo yum install -y mongo-10gen mongo-10gen-server
sudo easy_install simple_json
git clone https://github.com/xcu/pajarillos_analyser.git
sudo easy_install pip
sudo pip install django
export PYTHONPATH=$PYTHONPATH:~/pajarillos_analyser/pajarillos_analyser/
sudo service mongod start
