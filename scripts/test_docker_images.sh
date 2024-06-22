#!/usr/bin/env bash


TEST="./build/release/duckdb -c 'PRAGMA platform;' && make clean && echo 'DOCKER TEST RESULT: SUCCESS' || (echo 'DOCKER TEST RESULT: FAILURE' && make clean)"

make clean

# Currently not working due to cmake version being too low
# docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb amazonlinux:2 <<< "yum install gcc gcc-c++ git make cmake ninja-build -y && GEN=ninja make && $TEST" 2>&1

docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb alpine:latest <<< "apk add g++ git make cmake ninja python3 && cmake -Bbuild . && cmake --build build && cmake --install build && g++ -std=c++11 examples/embedded-c++/main.cpp"
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb amazonlinux:latest <<< "yum install clang git make cmake ninja-build -y && GEN=ninja make && $TEST" 2>&1
docker run -i --platform linux/arm64 --rm -v $(pwd):/duckdb --workdir /duckdb alpine:latest <<< "apk add g++ git make cmake ninja && GEN=ninja make && $TEST" 2>&1
docker run -i --platform linux/amd64 --rm -v $(pwd):/duckdb --workdir /duckdb alpine:latest <<< "apk add g++ git make cmake ninja && GEN=ninja make && $TEST" 2>&1
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb alpine:latest <<< "apk add g++ git make cmake ninja && GEN=ninja make && $TEST" 2>&1
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb alpine:latest <<< "apk add g++ git make cmake ninja python3 && GEN=ninja make && $TEST" 2>&1
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb alpine:latest <<< "apk add g++ git make cmake ninja && CXX_STANDARD=23 GEN=ninja make && $TEST" 2>&1
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb ubuntu:20.04 <<< "apt-get update && export DEBIAN_FRONTEND=noninteractive && apt-get install g++ git make cmake ninja-build -y && GEN=ninja make && $TEST" 2>&1
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb ubuntu:devel <<< "apt-get update && export DEBIAN_FRONTEND=noninteractive && apt-get install g++ git make cmake ninja-build -y && GEN=ninja make && $TEST" 2>&1
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb centos <<< "sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* && sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* && yum install git make cmake clang -y && make && $TEST" 2>&1
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb fedora <<< "dnf install make cmake ninja-build gcc g++ -y && make && $TEST" 2>&1
