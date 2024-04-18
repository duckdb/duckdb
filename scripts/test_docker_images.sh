#!/usr/bin/env bash

make clean
docker run -i --rm -v $(pwd):/duckdb --workdir /duckdb alpine:latest <<< "apk add g++ git make cmake ninja && GEN=ninja make" 2>&1
echo "alpine:latest completed"
