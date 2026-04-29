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
if [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
  echo "No access key available"
  exit 0
fi

TARGET=$(git log -1 --format=%h)

if [ "${UPLOAD_ASSETS_TO_STAGING_TARGET:-}" ]; then
  TARGET="$UPLOAD_ASSETS_TO_STAGING_TARGET"
fi

# decide target for staging
if [ "${OVERRIDE_GIT_DESCRIBE:-}" ]; then
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

files_from="$(mktemp "${TMPDIR:-/tmp}/duckdb-staging-files.XXXXXX")"
cleanup() {
  rm -f "$files_from"
}
trap cleanup EXIT
printf '%s\n' "${@:2}" > "$files_from"

rclone_remote=":s3,provider=AWS,endpoint=${AWS_ENDPOINT_URL},access_key_id=${AWS_ACCESS_KEY_ID},secret_access_key=${AWS_SECRET_ACCESS_KEY}"
if [ -n "${AWS_SESSION_TOKEN:-}" ]; then
  rclone_remote="${rclone_remote},session_token=${AWS_SESSION_TOKEN}"
fi

rclone $DRY_RUN_PARAM copy \
  --files-from "$files_from" \
  . \
  "${rclone_remote}:duckdb-staging/$TARGET/$GITHUB_REPOSITORY/$FOLDER/"
