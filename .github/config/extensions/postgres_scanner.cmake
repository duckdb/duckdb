# Note: tests for postgres_scanner are currently not run. All of them need a postgres server running. One test
#       uses a remote rds server but that's not something we want to run here.
if (NOT MINGW AND NOT ${WASM_ENABLED})
    # SereneDB vendors duckdb_postgres as a submodule at
    # third_party/duckdb_postgres (sibling of the duckdb submodule).
    duckdb_extension_load(postgres_scanner
            SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../duckdb_postgres
            )
endif()
