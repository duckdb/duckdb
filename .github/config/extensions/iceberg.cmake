if (NOT MINGW)
  duckdb_extension_load(iceberg
      LOAD_TESTS
      GIT_URL https://github.com/duckdb/duckdb-iceberg
      GIT_TAG 38c7671825d945ec7a511352baafb431e9c325db
  )
endif()
