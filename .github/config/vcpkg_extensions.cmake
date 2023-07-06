#
# This is the DuckDB extension config for extensions that require VCPKG as it will run on the CI.
#
# to build duckdb with this configuration run:
#   EXTENSION_CONFIGS=.github/config/vcpkg_extensions.cmake make
#

duckdb_extension_load(azure
    LOAD_TESTS
    GIT_URL https://github.com/samansmink/azure-extension
    GIT_TAG 8b6f1062988cce067a54c9f1a93bf0874531a675
)

duckdb_extension_load(aws
    LOAD_TESTS
    GIT_URL https://github.com/samansmink/aws
    GIT_TAG ff226f67941d71f9e20eaba031d1088366be75b1
)

duckdb_extension_load(iceberg
    LOAD_TESTS
    GIT_URL git@github.com:samansmink/duckdb-iceberg.git
    GIT_TAG a76546869e69d5a32e4089856814fbd537669115
)