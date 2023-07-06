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
    GIT_URL https://github.com/samansmink/aws
    GIT_TAG ff226f67941d71f9e20eaba031d1088366be75b1
)

#duckdb_extension_load(iceberg
#    LOAD_TESTS
#    GIT_URL git@github.com:samansmink/duckdb-iceberg.git
#    GIT_TAG a2668326dbc402b463ba0d80daacff87eb1ee59b
#)