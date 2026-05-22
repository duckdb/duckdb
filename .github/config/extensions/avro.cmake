if (NOT MINGW)
    duckdb_extension_load(avro
            SOURCE_DIR ${CMAKE_SOURCE_DIR}/third_party/duckdb_avro
            INCLUDE_DIR ${CMAKE_SOURCE_DIR}/third_party/duckdb_avro/src/include
    )
endif()
