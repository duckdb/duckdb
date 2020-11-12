#!/usr/bin/env bash

source scripts/install_node.sh $1
cd tools/nodejs
make clean
./configure
npm install --build-from-source
npm test
export PATH=node_modules/node-pre-gyp/bin:$PATH
node-pre-gyp package testpackage testbinary
node-pre-gyp publish
node-pre-gyp info