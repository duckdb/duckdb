#
# This is the DuckDB out-of-tree extension config as it will run on the CI
#
# to build duckdb with this configuration run:
#   EXTENSION_CONFIGS=.github/config/extension_config.cmake make
#

duckdb_extension_load(sqlite_scanner
    DONT_LINK
    GIT_URL https://github.com/duckdblabs/sqlite_scanner
    GIT_TAG e607f30160260a5d3152087e001967ece39c36c0
)
duckdb_extension_load(postgres_scanner
    DONT_LINK
    GIT_URL https://github.com/duckdblabs/postgres_scanner
    GIT_TAG cd043b49cdc9e0d3752535b8333c9433e1007a48
)
duckdb_extension_load(spatial
    DONT_LINK
    GIT_URL https://github.com/duckdblabs/duckdb_spatial.git
    GIT_TAG eaa44f9733b7f4a9f53791536ed8219ef39813f5
)

if (NOT WIN32)
    duckdb_extension_load(arrow
        DONT_LINK
        GIT_URL https://github.com/duckdblabs/arrow
        GIT_TAG c80462e30b463c2391e033fe11d86668a5ac44c3
    )
    duckdb_extension_load(substrait
        DONT_LINK
        GIT_URL https://github.com/duckdblabs/substrait
        GIT_TAG 48d64b27c7a6985d6f6c3e65044038544608c03b
    )
endif()