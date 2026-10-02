# Note: tests for postgres_scanner are currently not run. All of them need a postgres server running. One test
#       uses a remote rds server but that's not something we want to run here.
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(postgres_scanner
            GIT_URL https://github.com/duckdb/duckdb-postgres
            GIT_TAG f9db66ec5a5c35a30ce868d2b7233ffc0a21a08e
            SUBMODULES database-connector
            )
 endif()
