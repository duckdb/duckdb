#
# This is the DuckDB in-tree extension config as it will run on the CI
#
# to build duckdb with this configuration run:
#   EXTENSION_CONFIGS=.github/config/in_tree_extensions.cmake make
#

set(DUCKDB_MAIN_EXTENSION_CONFIG_TYPE "IN_TREE")
include("${CMAKE_CURRENT_LIST_DIR}/core.cmake")
include("${CMAKE_CURRENT_LIST_DIR}/cloud.cmake")
unset(DUCKDB_MAIN_EXTENSION_CONFIG_TYPE)
