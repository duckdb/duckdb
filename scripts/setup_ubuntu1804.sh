#!/bin/bash

# stuff
apt-get update -y -qq
apt-get install -y -qq software-properties-common
add-apt-repository ppa:git-core/ppa
apt-get update -y -qq
apt-get install -y -qq --fix-missing ninja-build make gcc-multilib g++-multilib libssl-dev wget openjdk-8-jdk zip maven unixodbc-dev libc6-dev-i386 lib32readline6-dev libssl-dev libcurl4-gnutls-dev libexpat1-dev gettext unzip build-essential checkinstall libffi-dev curl libz-dev openssh-client pkg-config

# cross compilation stuff
apt-get install -y -qq gcc-aarch64-linux-gnu g++-aarch64-linux-gnu

# git
wget https://github.com/git/git/archive/refs/tags/v2.18.5.tar.gz
tar xvf v2.18.5.tar.gz
cd git-2.18.5
make
make prefix=/usr install
git --version

# cmake
wget https://github.com/Kitware/CMake/releases/download/v3.21.3/cmake-3.21.3-linux-x86_64.sh
chmod +x cmake-3.21.3-linux-x86_64.sh
./cmake-3.21.3-linux-x86_64.sh --skip-license --prefix=/usr/local
cmake --version