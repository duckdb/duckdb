# Note: tests for postgres_scanner are currently not run. All of them need a postgres server running. One test
#       uses a remote rds server but that's not something we want to run here.
# Temporarily disabled
# if (NOT MINGW AND NOT ${WASM_ENABLED})
#     duckdb_extension_load(postgres_scanner
#             DONT_LINK
#             GIT_URL https://github.com/duckdb/duckdb-postgres
#             GIT_TAG f77b0cb511748fd70fb8a4eb265e2990599d286c
#             SUBMODULES database-connector
#             APPLY_PATCHES
#             )
# endif()
