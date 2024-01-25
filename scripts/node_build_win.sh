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
npx node-pre-gyp package testpackage testbinary

if [[ "$GITHUB_REF" =~ ^(refs/heads/main|refs/tags/v.+)$ ]] ; then
  npx node-pre-gyp publish
  npx node-pre-gyp info
fi