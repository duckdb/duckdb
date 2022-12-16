#!/usr/bin/env bash

set -e

TARGET_ARCH=${TARGET_ARCH:=x64}

source scripts/install_node.sh $1
cd tools/nodejs
make clean
./configure

if [[ "$1" == "15" ]] ; then
  # force upgrade npm's internal copy of node-gyp
  npm explore npm/node_modules/@npmcli/run-script -g -- npm_config_global=false npm install node-gyp@latest
fi

npm install --build-from-source --target_arch="$TARGET_ARCH"

./node_modules/.bin/node-pre-gyp reveal --target_arch="$TARGET_ARCH"

if [[ "$TARGET_ARCH" != "arm64" ]] ; then
  npm test
fi

export PATH=$(npm bin):$PATH
./node_modules/.bin/node-pre-gyp package testpackage testbinary --target_arch="$TARGET_ARCH"
if [[ "$GITHUB_REF" =~ ^(refs/heads/master|refs/tags/v.+)$ ]] ; then
  ./node_modules/.bin/node-pre-gyp publish --target_arch=$TARGET_ARCH
  ./node_modules/.bin/node-pre-gyp info --target_arch=$TARGET_ARCH
fi
