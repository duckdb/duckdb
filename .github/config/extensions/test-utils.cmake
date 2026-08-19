duckdb_extension_load(test_utils
  GIT_URL https://github.com/duckdb/bwc-test-utils
  # Use the commit before "Update extensions" (that contains the binaries of
  # the commit before that).
  GIT_TAG 61e38820957b51bfbc734e8b6c0403feb7e4883b
  APPLY_PATCHES
  # For local dev:
  # SOURCE_DIR "${EXTENSION_CONFIG_BASE_DIR}/../../../../test-utils"
)

include("${EXTENSION_CONFIG_BASE_DIR}/../in_tree_extensions.cmake")
