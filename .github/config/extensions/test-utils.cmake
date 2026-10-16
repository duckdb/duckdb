duckdb_extension_load(test_utils
  GIT_URL https://github.com/duckdb/bwc-test-utils
  # Use the commit before "Update extensions" (that contains the binaries of
  # the commit before that).
  GIT_TAG 3b28a1c56a26e2170e5cac5d8eeec165e007bc68
  APPLY_PATCHES
  # For local dev:
  # SOURCE_DIR "${EXTENSION_CONFIG_BASE_DIR}/../../../../test-utils"
)

include("${EXTENSION_CONFIG_BASE_DIR}/../in_tree_extensions.cmake")
