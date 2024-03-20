#!/bin/bash

# Main extension uploading script

# Usage: ./scripts/upload-staging-asset.sh <folder> <file>*
# <folder>              : Folder to upload to
# <file>                : File to be uploaded

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: ./scripts/upload-staging-asset.sh <folder> <file1> [... <fileN>]"
    exit 1
fi

set -e

FOLDER="$1"
DRY_RUN_PARAM=""

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

if [ "$GITHUB_EVENT_NAME" == "workflow_dispatch" ]; then
  echo "overriding DRY_RUN_PARAM, forcing upload"
  DRY_RUN_PARAM=""
fi

# dryrun if AWS key is not set
if [ -z "$AWS_ACCESS_KEY_ID" ]; then
  echo "No access key available"
  DRY_RUN_PARAM="--dryrun"
fi

# decide target for staging
if [ -z "$OVERRIDE_GIT_DESCRIBE" ]; then
  TARGET=$(git describe --tags --long)
else
  TARGET="$OVERRIDE_GIT_DESCRIBE"
fi

python3 -m pip install awscli

for var in "${@: 2}"
do
    aws s3 cp $var s3://duckdb-staging/$TARGET/$GITHUB_REPOSITORY/$FOLDER/ $DRY_RUN_PARAM --region us-east-2
done
