#
# This is the DuckDB extension config for extensions that require VCPKG as it will run on the CI.
#
# to build duckdb with this configuration run:
#   EXTENSION_CONFIGS=.github/config/vcpkg_extensions.cmake make
#

duckdb_extension_load(azure
    LOAD_TESTS
    GIT_URL https://github.com/duckdblabs/duckdb_azure
    GIT_TAG 7cd5149ee879f3ea9e0a9215e0739643dd75eb6e
)

duckdb_extension_load(aws
    LOAD_TESTS
    GIT_URL https://github.com/duckdblabs/duckdb_aws
    GIT_TAG 617a4b1456eec1dee3d668f9ce005a1de9ef21c8
)