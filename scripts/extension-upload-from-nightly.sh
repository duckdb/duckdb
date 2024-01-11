#!/bin/bash

# This script deploys the extension binaries that are currently deployed to the nightly bucket to the main bucket

# WARNING: don't use this script if you don't know exactly what you're doing. To deploy a binary:
# - Run the script with ./extension-upload-from-nightly.sh <extension_name> <duckdb_version> (<nightly_commit>)
# - CHECK the output of the dry run thoroughly
# - If successful, set the I_KNOW_WHAT_IM_DOING_DEPLOY_FOR_REAL env variable to the correct value
# - run the script again now deploying for real
# - check the output
# - unset the I_KNOW_WHAT_IM_DOING_DEPLOY_FOR_REAL env var

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: ./extension-upload-from-nightly.sh <extension_name> <duckdb_version> (<nightly_commit>)"
    exit 1
fi

if [ -z "$3" ]; then
    BASE_NIGHTLY_DIR="$2"
else
    BASE_NIGHTLY_DIR="$1/$3/$2"
fi

# CONFIG
FROM_BUCKET=duckdb-extensions-nightly
TO_BUCKET=duckdb-extensions
CLOUDFRONT_DISTRIBUTION_ID=E2Z28NDMI4PVXP

### COPY THE FILES
REAL_RUN="aws s3 cp s3://$FROM_BUCKET/$BASE_NIGHTLY_DIR s3://$TO_BUCKET/$2 --recursive --exclude '*' --include '*/$1.duckdb_extension.gz' --acl public-read"
DRY_RUN="$REAL_RUN --dryrun"

if [ "$I_KNOW_WHAT_IM_DOING_DEPLOY_FOR_REAL" == "yessir" ]; then
  echo "DEPLOYING"
  echo "> FROM: $FROM_BUCKET"
  echo "> TO  : $TO_BUCKET"
  echo "> AWS CLI deploy: "
  eval "$REAL_RUN"
else
  echo "DEPLOYING (DRY RUN)"
  echo "> FROM: $FROM_BUCKET"
  echo "> TO  : $TO_BUCKET"
  echo "> AWS CLI Dry run: "
  eval "$DRY_RUN"
fi

echo ""

### INVALIDATE THE CLOUDFRONT CACHE
# For double checking we are invalidating the correct domain
CLOUDFRONT_ORIGINS=`aws cloudfront get-distribution --id $CLOUDFRONT_DISTRIBUTION_ID --query 'Distribution.DistributionConfig.Origins.Items[*].DomainName' --output text`

# Parse the dry run output
output=$(eval "$DRY_RUN")
s3_paths=()
while IFS= read -r line; do
    if [[ $line == *"copy:"* ]]; then
        s3_path=$(echo $line | grep -o 's3://[^ ]*' | awk 'NR%2==0' | awk -F "s3://$TO_BUCKET" '{print $2}' | cut -d' ' -f1)
        s3_paths+=("$s3_path")
    fi
done <<< "$output"

if [ "$I_KNOW_WHAT_IM_DOING_DEPLOY_FOR_REAL" == "yessir" ]; then
  echo "INVALIDATION"
  echo "> Total files: ${#s3_paths[@]}"
  echo "> Domain: $CLOUDFRONT_ORIGINS"
  for path in "${s3_paths[@]}"; do
    aws cloudfront create-invalidation --distribution-id "$CLOUDFRONT_DISTRIBUTION_ID" --paths "$path"
  done
else
  echo "INVALIDATION (DRY RUN)"
  echo "> Total files: ${#s3_paths[@]}"
  echo "> Domain: $CLOUDFRONT_ORIGINS"
  echo "> Paths:"
  for path in "${s3_paths[@]}"; do
    echo "    $path"
  done
fi