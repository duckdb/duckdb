if (NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR MUSL_ENABLED)
    return()
endif()

duckdb_extension_load(sirius
    GIT_URL https://github.com/sirius-db/sirius
    GIT_TAG e6a43ebcde9d04524a80aa059e06727c03da578d
    SOURCE_DIR sirius-duckdb/loader
    INCLUDE_DIR sirius-duckdb/loader/src
    SUBMODULES ""
)
