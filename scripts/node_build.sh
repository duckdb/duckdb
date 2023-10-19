#!/usr/bin/env bash

set -ex

TARGET_ARCH=${TARGET_ARCH:=x64}
echo targeting arch: $TARGET_ARCH

set +x
source scripts/install_node.sh $1
set -x
cd tools/nodejs
make clean
./configure

if [ "$(expr substr $(uname -s) 1 5)" == "Linux" ] && [[ "$TARGET_ARCH" == "arm64" ]] ; then
  sudo apt-get install gcc-aarch64-linux-gnu g++-aarch64-linux-gnu --yes
  export CC=aarch64-linux-gnu-gcc
  export CXX=aarch64-linux-gnu-g++
fi

npm install --build-from-source --target_arch="$TARGET_ARCH"

./node_modules/.bin/node-pre-gyp reveal --target_arch="$TARGET_ARCH"

if [[ "$TARGET_ARCH" != "arm64" ]] ; then
  npm test
else
  ARCH=$(file lib/binding/duckdb.node | tr '[:upper:]' '[:lower:]')
  if [[ "$ARCH" != *"arm"* ]] ; then
    echo "no arch $ARCH"
    exit 1
  fi
fi

export PATH=$(npm bin):$PATH
./node_modules/.bin/node-pre-gyp package testpackage testbinary --target_arch="$TARGET_ARCH"
if [[ "$GITHUB_REF" =~ ^(refs/heads/main|refs/tags/v.+)$ ]] ; then
  ./node_modules/.bin/node-pre-gyp publish --target_arch=$TARGET_ARCH
  ./node_modules/.bin/node-pre-gyp info --target_arch=$TARGET_ARCH
fi
