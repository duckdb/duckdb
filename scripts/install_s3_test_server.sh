#!/usr/bin/env bash
# Note: needs sudo

unamestr=$(uname)
if [[ "$unamestr" == 'Linux' ]]; then
	apt-get install -y docker.io
fi

docker --version
echo '127.0.0.1 duckdb-minio.com' >> /etc/hosts
echo '127.0.0.1 test-bucket.duckdb-minio.com' >> /etc/hosts
echo '127.0.0.1 test-bucket-2.duckdb-minio.com' >> /etc/hosts
echo '127.0.0.1 test-bucket-public.duckdb-minio.com' >> /etc/hosts