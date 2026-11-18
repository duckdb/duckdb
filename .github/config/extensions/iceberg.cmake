# Windows tests for iceberg currently not working
IF (NOT WIN32)
  set(LOAD_ICEBERG_TESTS "LOAD_TESTS")
else ()
  set(LOAD_ICEBERG_TESTS "")
endif()
if (NOT MINGW)
  duckdb_extension_load(iceberg
            ${LOAD_ICEBERG_TESTS}
            GIT_URL https://github.com/duckdb/duckdb-iceberg
            GIT_TAG f979b5b28a044c49c38a26636ac01bc1f7047fa5
            )
endif()
