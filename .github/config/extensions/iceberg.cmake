if (NOT MINGW)
  duckdb_extension_load(iceberg
      LOAD_TESTS
      GIT_URL https://github.com/duckdb/duckdb-iceberg
      GIT_TAG 34215811ba674fb029fb3761e170aefaec4bfc47
  )
endif()
