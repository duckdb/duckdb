#!/usr/bin/env bash

set -e

node --version
npm --version
which node

cd tools/nodejs
make clean
./configure

npm install --build-from-source
npm test
export PATH=$(npm bin):$PATH
node-pre-gyp package testpackage testbinary
if [[ "$GITHUB_REF" =~ ^(refs/heads/master|refs/tags/v.+)$ ]] ; then
  node-pre-gyp publish
  node-pre-gyp info
fi