#
# This config file holds all out-of-tree extension that are built with DuckDB's CI
#
# to build duckdb with this configuration run:
#   EXTENSION_CONFIGS=.github/config/out_of_tree_extensions.cmake make
#
#  Note that many of these packages require vcpkg, and a merged manifest must be created to
#  compile multiple of them.
#
#  After setting up vcpkg, build using e.g. the following commands:
#  USE_MERGED_VCPKG_MANIFEST=1 BUILD_ALL_EXT=1 make extension_configuration
#  USE_MERGED_VCPKG_MANIFEST=1 BUILD_ALL_EXT=1 make debug
#
#  Make sure the VCPKG_TOOLCHAIN_PATH and VCPKG_TARGET_TRIPLET are set. For example:
#  VCPKG_TOOLCHAIN_PATH=~/vcpkg/scripts/buildsystems/vcpkg.cmake
#  VCPKG_TARGET_TRIPLET=arm64-osx

include("${EXTENSION_CONFIG_BASE_DIR}/avro.cmake")
include("${EXTENSION_CONFIG_BASE_DIR}/aws.cmake")
include("${EXTENSION_CONFIG_BASE_DIR}/azure.cmake")
include("${EXTENSION_CONFIG_BASE_DIR}/ducklake.cmake")
include("${EXTENSION_CONFIG_BASE_DIR}/encodings.cmake")
include("${EXTENSION_CONFIG_BASE_DIR}/excel.cmake")
include("${EXTENSION_CONFIG_BASE_DIR}/fts.cmake")
include("${EXTENSION_CONFIG_BASE_DIR}/httpfs.cmake")
include("${EXTENSION_CONFIG_BASE_DIR}/iceberg.cmake")
include("${EXTENSION_CONFIG_BASE_DIR}/inet.cmake")
include("${EXTENSION_CONFIG_BASE_DIR}/mysql_scanner.cmake")
include("${EXTENSION_CONFIG_BASE_DIR}/postgres_scanner.cmake")
# include("${EXTENSION_CONFIG_BASE_DIR}/spatial.cmake") Remove spatial until the geometry refactor is done
include("${EXTENSION_CONFIG_BASE_DIR}/sqlite_scanner.cmake")
include("${EXTENSION_CONFIG_BASE_DIR}/sqlsmith.cmake")
include("${EXTENSION_CONFIG_BASE_DIR}/vss.cmake")
