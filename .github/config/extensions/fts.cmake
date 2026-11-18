duckdb_extension_load(fts
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-fts
        GIT_TAG 623ee80e0bd5a771347f7dd320915cc3ef2fb965
        TEST_DIR test/sql
        APPLY_PATCHES
)
