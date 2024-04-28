#!/usr/bin/env bash

make clean
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb amazonlinux:2 <<< "yum install g++ git make cmake ninja-build -y && GEN=ninja make && make clean" 2>&1
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb amazonlinux:latest <<< "yum install clang git make cmake ninja-build -y && GEN=ninja make && make clean" 2>&1
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb alpine:latest <<< "apk add g++ git make cmake ninja && GEN=ninja make && make clean" 2>&1
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb alpine:latest <<< "apk add g++ git make cmake ninja python3 && GEN=ninja make && make clean" 2>&1
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb alpine:latest <<< "apk add g++ git make cmake ninja && CXX_STANDARD=23 GEN=ninja make && make clean" 2>&1
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb ubuntu:20.04 <<< "apt-get update && export DEBIAN_FRONTEND=noninteractive && apt-get install g++ git make cmake ninja-build -y && GEN=ninja make && make clean" 2>&1
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb ubuntu:devel <<< "apt-get update && export DEBIAN_FRONTEND=noninteractive && apt-get install g++ git make cmake ninja-build -y && GEN=ninja make && make clean" 2>&1
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb centos <<< "sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* && sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* && yum install git make cmake clang -y && make && make clean" 2>&1
