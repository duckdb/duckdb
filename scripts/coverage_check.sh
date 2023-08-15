#!/bin/bash

set -e

# prepare coverage file
lcov --config-file .github/workflows/lcovrc --zerocounters --directory .
lcov --config-file .github/workflows/lcovrc --capture --initial --directory . --base-directory . --no-external --output-file base_coverage.info

# run tests
build/coverage/test/unittest
build/coverage/test/unittest "[coverage]"
build/coverage/test/unittest "[intraquery]"
build/coverage/test/unittest "[interquery]"
build/coverage/test/unittest "[detailed_profiler]"
build/coverage/test/unittest test/sql/tpch/tpch_sf01.test_slow
python3 tools/shell/shell-test.py build/coverage/duckdb

# finalize coverage file
lcov --config-file .github/workflows/lcovrc --directory . --base-directory . --no-external --capture  --output-file test_coverage.info
lcov -a base_coverage.info -a test_coverage.info -o total_coverage.info
lcov --config-file .github/workflows/lcovrc --remove total_coverage.info $(< .github/workflows/lcov_exclude) -o lcov.info

# generate coverage html
genhtml -o coverage_html lcov.info

# check that coverage passes threshold
python3 scripts/check_coverage.py
