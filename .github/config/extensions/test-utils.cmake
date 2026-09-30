duckdb_extension_load(test_utils
  GIT_URL https://github.com/duckdb/bwc-test-utils
  # Use the commit before "Update extensions" (that contains the binaries of
  # the commit before that).
  GIT_TAG b7bef9e85c190a090c11b3a58b419a8487465083
  APPLY_PATCHES
  # For local dev:
  # SOURCE_DIR "${EXTENSION_CONFIG_BASE_DIR}/../../../../test-utils"
)

include("${EXTENSION_CONFIG_BASE_DIR}/../in_tree_extensions.cmake")
