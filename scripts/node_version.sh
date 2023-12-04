#!/usr/bin/env bash

set -ex

cd tools/nodejs
./configure

export TAG=''
# for main do prereleases
if [[ "$GITHUB_REF" =~ ^refs/tags/v.+$ ]] ; then
	# proper release
	npm version `echo $GITHUB_REF | sed 's|refs/tags/v||'`
else
	git describe --tags --long || exit

	export VER=`git describe --tags --abbrev=0 | tr -d "v"`
	export DIST=`git describe --tags --long | cut -f2 -d-`

	# set version to lastver
	npm version $VER
	npm version prerelease --preid="dev"$DIST
	export TAG='--tag next'
fi

npm pack --dry-run

# upload to npm, maybe
if [[ "$GITHUB_REF" =~ ^(refs/heads/main|refs/tags/v.+)$ && "$1" = "upload" ]] ; then
	npm config set //registry.npmjs.org/:_authToken $NODE_AUTH_TOKEN
	npm publish --access public $TAG
fi
