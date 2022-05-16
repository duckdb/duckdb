#!/usr/bin/env bash
# Note: needs sudo

apt-get install -y docker.io
docker --version
echo '127.0.0.1 duckdb-minio.com' >> /etc/hosts
echo '127.0.0.1 test-bucket.duckdb-minio.com' >> /etc/hosts
echo '127.0.0.1 test-bucket-public.duckdb-minio.com' >> /etc/hosts