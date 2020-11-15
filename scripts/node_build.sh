#!/usr/bin/env bash

source scripts/install_node.sh $1
cd tools/nodejs
make clean
./configure

# figure out the version number from the tag
export LASTTAG=`git tag -l --sort tag | tail -n 1`
export LASTVER=`echo $LASTTAG | tr -d "v"`
npm version $LASTVER
# TODO when we're on the tag build, dont do this
export MASTERDIST=`git log $LASTTAG..HEAD --pretty=oneline | wc -l | tr -d " "`
npm version prerelease --preid=$MASTERDIST
npm publish --tag next --dry-run


npm install --build-from-source
npm test
export PATH=node_modules/node-pre-gyp/bin:$PATH
node-pre-gyp package testpackage testbinary
if [[ "$GITHUB_REF" =~ ^(refs/heads/master|refs/tags/v.+)$ ]] ; then
  node-pre-gyp publish
  node-pre-gyp info
fi