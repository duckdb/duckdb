duckdb_extension_load(test_utils
  GIT_URL https://github.com/duckdb/bwc-test-utils
  GIT_TAG df0ca97925bc8b3349ee7b1d0ad7496af077daf8
  # For local dev:
  # SOURCE_DIR "${EXTENSION_CONFIG_BASE_DIR}/../../../../test-utils"
)

include("${EXTENSION_CONFIG_BASE_DIR}/../in_tree_extensions.cmake")
