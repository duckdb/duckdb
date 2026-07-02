duckdb_extension_load(test_utils
  GIT_URL https://github.com/duckdb/bwc-test-utils
  GIT_TAG 5b9c7334949c47cdf69ce11c151139c4c88aa7f8
  # For local dev:
  # SOURCE_DIR "${EXTENSION_CONFIG_BASE_DIR}/../../../../test-utils"
)

include("${EXTENSION_CONFIG_BASE_DIR}/../in_tree_extensions.cmake")
