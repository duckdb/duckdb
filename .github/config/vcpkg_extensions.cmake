#
# This is the DuckDB extension config for extensions that require VCPKG as it will run on the CI.
#
# to build duckdb with this configuration run:
#   EXTENSION_CONFIGS=.github/config/vcpkg_extensions.cmake make
#

duckdb_extension_load(azure
    LOAD_TESTS
    GIT_URL https://github.com/samansmink/azure-extension
    GIT_TAG 8441330242fa8b84fbaebbe478c46b46db408bf8
)

duckdb_extension_load(aws
    LOAD_TESTS
    GIT_URL https://github.com/samansmink/aws
    GIT_TAG c7a91ff4af3d17e0317359de6b2aad4dd1d01dbb
)