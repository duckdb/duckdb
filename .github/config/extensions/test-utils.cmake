duckdb_extension_load(test_utils
  GIT_URL https://github.com/duckdb/bwc-test-utils
  GIT_TAG 7074208283523a3af8b5ddd1c890a03abdba3b9b
  # For local dev:
  # SOURCE_DIR "${EXTENSION_CONFIG_BASE_DIR}/../../../../test-utils"
)

include("${EXTENSION_CONFIG_BASE_DIR}/../in_tree_extensions.cmake")
