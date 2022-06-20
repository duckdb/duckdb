#!/usr/bin/env bash

docker compose -f scripts/minio_s3.yml -p duckdb-minio up -d
