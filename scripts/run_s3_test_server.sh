#!/usr/bin/env bash

mkdir -p /tmp/minio_test_data
mkdir -p /tmp/minio_root_data
docker compose -f scripts/minio_s3.yml -p duckdb-minio up -d
