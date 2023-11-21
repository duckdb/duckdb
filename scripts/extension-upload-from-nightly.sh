#!/bin/bash

# This script deploys the extension binaries that are currently deployed to the nightly bucket to the main bucket

# WARNING: don't use this script if you don't know exactly what you're doing. To deploy a binary:
# - Run the script with ./extension-upload-from-nightly.sh <extension_name> <duckdb_version> (<nightly_commit>)
# - CHECK the output of the dry run thoroughly
# - If successful, set the I_KNOW_WHAT_IM_DOING_DEPLOY_FOR_REAL env variable to the correct value
# - run the script again now deploying for real
# - check the output
# - unset the I_KNOW_WHAT_IM_DOING_DEPLOY_FOR_REAL env var

# TODO: make the script invalidate the cloudfront cache too

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: ./extension-upload-from-nightly.sh <extension_name> <duckdb_version> (<nightly_commit>)"
    exit 1
fi

if [ -z "$3" ]; then
    BASE_NIGHTLY_DIR="$2"
else
    BASE_NIGHTLY_DIR="$1/$3/$2"
fi

REAL_RUN="aws s3 cp s3://duckdb-extensions-nightly/$BASE_NIGHTLY_DIR s3://duckdb-extensions/$2 --recursive --exclude '*' --include '*/$1.duckdb_extension.gz' --acl public-read"
DRY_RUN="$REAL_RUN --dryrun"

if [ "$I_KNOW_WHAT_IM_DOING_DEPLOY_FOR_REAL" == "yessir" ]; then
  echo "DEPLOYING `eval "$DRY_RUN" | wc -l` FILES"
  eval "$REAL_RUN"
else
  echo "DRY RUN FOR `eval "$DRY_RUN" | wc -l` FILES"
  eval "$DRY_RUN"
fi

echo "Done!"