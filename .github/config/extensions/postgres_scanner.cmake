if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(postgres_scanner
            SOURCE_DIR ${CMAKE_SOURCE_DIR}/third_party/duckdb_postgres
            INCLUDE_DIR ${CMAKE_SOURCE_DIR}/third_party/duckdb_postgres/src/include
    )
endif()
