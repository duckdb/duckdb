#!/usr/bin/env bash
s3rver -d "/tmp/s3rver" -p "4923" -a 127.0.0.1 --service-endpoint 's3rver-endpoint.com' --configure-bucket "test-bucket"