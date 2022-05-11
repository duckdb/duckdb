#!/usr/bin/env bash
# Note: needs sudo for editing hosts file

npm install -g s3rver
echo '127.0.0.1 test-bucket.s3.s3rver-endpoint.com' >> /etc/hosts
echo 's3.s3rver-endpoint.com' >> /etc/hosts