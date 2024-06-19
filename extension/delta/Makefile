PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=deltatable
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Set test paths
test_release: export DELTA_KERNEL_TESTS_PATH=./build/release/rust/src/delta_kernel/kernel/tests/data
test_release: export DAT_PATH=./build/release/rust/src/delta_kernel/acceptance/tests/dat

test_debug: export DELTA_KERNEL_TESTS_PATH=./build/debug/rust/src/delta_kernel/kernel/tests/data
test_debug: export DAT_PATH=./build/debug/rust/src/delta_kernel/acceptance/tests/dat

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

reldebug:
	mkdir -p build/reldebug && \
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) -DCMAKE_BUILD_TYPE=RelWithDebInfo -S ./duckdb/ -B build/reldebug && \
	cmake --build build/reldebug --config RelWithDebInfo

# Generate some test data to test with
generate-data:
	python3 -m pip install delta-spark duckdb pandas deltalake pyspark delta
	python3 scripts/generate_test_data.py
