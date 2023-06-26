#!/bin/bash

# prepare coverage file
lcov --config-file .github/workflows/lcovrc --zerocounters --directory .
lcov --config-file .github/workflows/lcovrc --capture --initial --directory . --base-directory . --no-external --output-file coverage.info

# build with coverage enabled
mkdir -p build/coverage
(cd build/coverage && cmake -E env CXXFLAGS="--coverage" cmake -DBUILD_PARQUET_EXTENSION=1 -DBUILD_ICU_EXTENSION=1 -DBUILD_JSON_EXTENSION=1 -DBUILD_JEMALLOC_EXTENSION=1 -DBUILD_AUTOCOMPLETE_EXTENSION=1 -DENABLE_SANITIZER=0 -DCMAKE_BUILD_TYPE=Debug ../.. && cmake --build .)

# run tests
build/coverage/test/unittest
build/coverage/test/unittest "[intraquery]"
build/coverage/test/unittest "[interquery]"
build/coverage/test/unittest "[detailed_profiler]"
build/coverage/test/unittest test/sql/tpch/tpch_sf01.test_slow
build/coverage/tools/sqlite3_api_wrapper/test_sqlite3_api_wrapper
python3 tools/shell/shell-test.py build/coverage/duckdb

# finalize coverage file
lcov --config-file .github/workflows/lcovrc --directory . --base-directory . --no-external --capture --output-file coverage.info
lcov --config-file .github/workflows/lcovrc --remove coverage.info $(< .github/workflows/lcov_exclude) -o lcov.info

# generate coverage html
genhtml -o coverage_html lcov.info

# check that coverage passes threshold
python3 scripts/check_coverage.py
