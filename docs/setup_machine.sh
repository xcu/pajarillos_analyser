#!/bin/bash

sudo easy_install oauth2
sudo easy_install ipython
sudo easy_install ujson
sudo easy_install pymongo
sudo yum install git
echo '[10gen]
name=10gen Repository
baseurl=http://downloads-distro.mongodb.org/repo/redhat/os/x86_64
gpgcheck=0
enabled=1' > /etc/yum.repos.d/10gen.repo
sudo yum install mongo-10gen mongo-10gen-server
git clone https://github.com/xcu/pajarillos_analyser.git

service mongod start
