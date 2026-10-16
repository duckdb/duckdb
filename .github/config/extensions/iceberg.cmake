if (NOT MINGW)
  duckdb_extension_load(iceberg
    GIT_URL https://github.com/duckdb/duckdb-iceberg
    GIT_TAG 10eafd0b340c946e4a6ba7b6c7244c6a270327b0
    APPLY_PATCHES
    )
endif()
