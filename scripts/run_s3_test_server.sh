#!/usr/bin/env bash
#Note: DONT run as root

set -e

if [ ! -f data/attach_test/attach.db ]; then
    echo "File data/attach_test/attach.db not found, run ./scripts/generate_presigned_url.sh to generate"
    exit 1
fi

rm -rf /tmp/minio_test_data
rm -rf /tmp/minio_root_data
mkdir -p /tmp/minio_test_data
mkdir -p /tmp/minio_root_data
docker compose -f scripts/minio_s3.yml -p duckdb-minio up -d

# for testing presigned url 
sleep 10
container_name=$(docker ps -a --format '{{.Names}}' | grep -m 1 "duckdb-minio")
echo $container_name

export S3_SMALL_CSV_PRESIGNED_URL=$(docker logs $container_name 2>/dev/null | grep -m 1 'Share:.*phonenumbers\.csv' | grep -o 'http[s]\?://[^ ]\+')
echo $S3_SMALL_CSV_PRESIGNED_URL

export S3_SMALL_PARQUET_PRESIGNED_URL=$(docker logs $container_name 2>/dev/null | grep -m 1 'Share:.*t1\.parquet' | grep -o 'http[s]\?://[^ ]\+')
echo $S3_SMALL_PARQUET_PRESIGNED_URL

export S3_LARGE_PARQUET_PRESIGNED_URL=$(docker logs $container_name 2>/dev/null | grep -m 1 'Share:.*lineitem_large\.parquet' | grep -o 'http[s]\?://[^ ]\+')
echo $S3_LARGE_PARQUET_PRESIGNED_URL

export S3_ATTACH_DB_PRESIGNED_URL=$(docker logs $container_name 2>/dev/null | grep -m 1 'Share:.*attach\.db' | grep -o 'http[s]\?://[^ ]\+')
echo $S3_ATTACH_DB_PRESIGNED_URL

export S3_ATTACH_DB="s3://test-bucket/presigned/attach.db"