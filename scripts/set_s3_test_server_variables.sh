#!/usr/bin/env bash

# Run this script with 'source' or the shorthand: '.':
# i.e: source scripts/set_s3_test_server_variables.sh

# Enable the S3 tests to run
export S3_TEST_SERVER_AVAILABLE=1

export AWS_DEFAULT_REGION=eu-west-1
export AWS_ACCESS_KEY_ID=minio_duckdb_user
export AWS_SECRET_ACCESS_KEY=minio_duckdb_user_password
export DUCKDB_S3_ENDPOINT=duckdb-minio.com:9000
export DUCKDB_S3_USE_SSL=false
