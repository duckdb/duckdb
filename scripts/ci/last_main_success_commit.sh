#!/usr/bin/env bash

set -euo pipefail

PAGER= gh run list \
  --repo duckdb/duckdb \
  --branch=main \
  --workflow=Main \
  --event=workflow_dispatch \
  --status=success \
  --json=headSha \
  --limit=1 \
  --jq '.[0].headSha'
