#!/usr/bin/env bash
#Note: DONT run as root

# 1. upload test files (duckdb/data/) to testing Minio server
# 2. use `usr/bin/mc share download` to generatd the presigned url
# 3. set presigned url to env variable, so sqllogictest can read the presigned url

# 1
build/release/test/unittest test/sql/copy/s3/s3_presigned_upload.test
build/release/test/unittest test/sql/copy/s3/s3_presigned_upload.test_slow

# 2
docker compose -f scripts/presigned_url.yml -p duckdb-mc up -d 
sleep 3

# 3 
export S3_SMALL_CSV_PRESIGNED_URL=$(docker logs duckdb-mc_minio_mc_1 | grep -m 1 'Share:.*web_page\.csv' | grep -o 'http[s]\?://[^ ]\+')
echo $S3_SMALL_CSV_PRESIGNED_URL

export S3_SMALL_PARQUET_PRESIGNED_URL=$(docker logs duckdb-mc_minio_mc_1 | grep -m 1 'Share:.*web_page\.parquet' | grep -o 'http[s]\?://[^ ]\+')
echo $S3_SMALL_PARQUET_PRESIGNED_URL

export S3_LARGE_PARQUET_PRESIGNED_URL=$(docker logs duckdb-mc_minio_mc_1 | grep -m 1 'Share:.*lineitem_large\.parquet' | grep -o 'http[s]\?://[^ ]\+')
echo $S3_LARGE_PARQUET_PRESIGNED_URL