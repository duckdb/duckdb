#!/usr/bin/env bash
# Note: needs sudo for editing hosts file

# Old s3rver config
#npm install -g s3rver
#echo '127.0.0.1 test-bucket.s3.s3rver-endpoint.com' >> /etc/hosts
#echo '127.0.0.1 s3.s3rver-endpoint.com' >> /etc/hosts

sudo apt-get install -y docker.io
docker --version
echo '127.0.0.1 duckdb-minio.com' >> /etc/hosts
echo '127.0.0.1 test-bucket.duckdb-minio.com' >> /etc/hosts
echo '127.0.0.1 test-bucket-public.duckdb-minio.com' >> /etc/hosts