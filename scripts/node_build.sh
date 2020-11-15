#!/usr/bin/env bash

source scripts/install_node.sh $1
cd tools/nodejs
make clean
./configure

# figure out the version number from the tag

git describe --tags --long  

export LASTVER=`git describe --tags --abbrev=0 | tr -d "v"`
export DIST=`git describe --tags --long | cut -f2 -d-`

# set version to lastver
npm version $LASTVER
# TODO when we're on the tag build, dont do this
npm version prerelease --preid="dev"$DIST
npm publish --tag next --dry-run


npm install --build-from-source
npm test
export PATH=node_modules/node-pre-gyp/bin:$PATH
node-pre-gyp package testpackage testbinary
if [[ "$GITHUB_REF" =~ ^(refs/heads/master|refs/tags/v.+)$ ]] ; then
  node-pre-gyp publish
  node-pre-gyp info
fi