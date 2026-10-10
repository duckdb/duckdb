if (NOT MINGW)
  duckdb_extension_load(iceberg
      LOAD_TESTS
      GIT_URL https://github.com/duckdb/duckdb-iceberg
      GIT_TAG e4950259ba8a3ac5b49b1cbb86e45be0215fead1
      APPLY_PATCHES
  )
endif()
