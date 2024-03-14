#!/bin/bash

# Main extension uploading script

# Usage: ./scripts/upload-staging-asset.sh <file>
# <file>                : File to be uploaded

set -e

DRY_RUN_PARAM=""

# dryrun if AWS key is not set
if [ -z "$AWS_ACCESS_KEY_ID" ]; then
  echo "No access key available"
  DRY_RUN_PARAM="--dryrun"
fi

# dryrun if repo is not duckdb/duckdb
if [ "$GITHUB_REPOSITORY" != "duckdb/duckdb" ]; then
  echo "Repository is $GITHUB_REPOSITORY (not duckdb/duckdb)"
  DRY_RUN_PARAM="--dryrun"
fi
# dryrun if we are not in main
if [ "$GITHUB_REF" != "refs/heads/main" ]; then
  echo "git ref is $GITHUB_REF (not refs/heads/main)"
  DRY_RUN_PARAM="--dryrun"
fi

# decide target for staging
if [ -z "$OVERRIDE_GIT_DESCRIBE" ]; then
  TARGET=$(git describe --tags --long)
else
  TARGET="$OVERRIDE_GIT_DESCRIBE"
fi

python3 -m pip install awscli

for var in "$@"
do
    aws s3 cp $var s3://duckdb-staging/duckdb/$TARGET/. $DRY_RUN_PARAM
done
