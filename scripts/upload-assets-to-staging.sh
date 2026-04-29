#!/bin/bash

# Main extension uploading script

# Usage: ./scripts/upload-staging-asset.sh <folder> <file>*
# <folder>              : Folder to upload to
# <file>                : File to be uploaded

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: ./scripts/upload-staging-asset.sh <folder> <file1> [... <fileN>]"
    exit 1
fi

set -eu -o pipefail

# skip if repo is not in duckdb organization
if [ "$GITHUB_REPOSITORY_OWNER" != "duckdb" ]; then
  echo "Repository is $GITHUB_REPOSITORY_OWNER (not duckdb)"
  exit 0
fi

FOLDER="$1"
DRY_RUN_PARAM=""

# dryrun if repo is not duckdb/duckdb
if [ "$GITHUB_REPOSITORY" != "duckdb/duckdb" ]; then
  echo "Repository is $GITHUB_REPOSITORY (not duckdb/duckdb)"
  DRY_RUN_PARAM="--dry-run"
fi
# dryrun if we are not in main
if [ "$GITHUB_REF" != "refs/heads/main" ]; then
  echo "git ref is $GITHUB_REF (not refs/heads/main)"
  DRY_RUN_PARAM="--dry-run"
fi

if [ "$GITHUB_EVENT_NAME" == "workflow_dispatch" ]; then
  echo "overriding DRY_RUN_PARAM, forcing upload"
  DRY_RUN_PARAM=""
fi

# dryrun if AWS key is not set
if [ -z "$AWS_ACCESS_KEY_ID" ]; then
  echo "No access key available"
  DRY_RUN_PARAM="--dry-run"
fi

TARGET=$(git log -1 --format=%h)

if [ "$UPLOAD_ASSETS_TO_STAGING_TARGET" ]; then
  TARGET="$UPLOAD_ASSETS_TO_STAGING_TARGET"
fi

# decide target for staging
if [ "$OVERRIDE_GIT_DESCRIBE" ]; then
  TARGET="$TARGET/$OVERRIDE_GIT_DESCRIBE"
fi

if ! command -v rclone >/dev/null 2>&1; then
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*)
      choco install rclone -y
      ;;
    *)
      install_runner=(bash)
      if command -v sudo >/dev/null 2>&1; then
        install_runner=(sudo bash)
      fi
      curl -fsSL --retry 5 https://rclone.org/install.sh | "${install_runner[@]}"
      ;;
  esac
fi

set -x

rclone $DRY_RUN_PARAM copy \
  --files-from <(printf '%s\n' "${@:2}") \
  . \
  ":s3,provider=AWS,env_auth=true,endpoint=${AWS_ENDPOINT_URL}:duckdb-staging/$TARGET/$GITHUB_REPOSITORY/$FOLDER/"
