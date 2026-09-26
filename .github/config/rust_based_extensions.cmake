#
# This is the extension configuration for extensions that require rust to build. This needs some extra setup in CI
# but also takes up a bit more disk space, which is why we run them in a separate build
#
# to build duckdb with this configuration run:
#   EXTENSION_CONFIGS=.github/config/rust-based-extensions.cmake make
#

################## DELTA
include("${EXTENSION_CONFIG_BASE_DIR}/delta.cmake")
duckdb_extension_statically_link(delta)
include("${EXTENSION_CONFIG_BASE_DIR}/unity_catalog.cmake")
duckdb_extension_statically_link(unity_catalog)
