#!/bin/sh
rm -rf dist
rm -rf src/duckdb

mkdir -p src/duckdb/third_party

cp ../../CMakeLists.txt src/duckdb
cp ../../third_party/CMakeLists.txt src/duckdb/third_party

cp -r ../../src src/duckdb

cp -r ../../third_party/libpg_query src/duckdb/third_party
cp -r ../../third_party/hyperloglog src/duckdb/third_party
cp -r ../../third_party/re2 src/duckdb/third_party
cp -r ../../third_party/miniz src/duckdb/third_party

python3 setup.py sdist
pip3 install twine

#twine upload --repository-url https://test.pypi.org/legacy/ dist/duckdb-*.tar.gz 
