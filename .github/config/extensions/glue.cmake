if (NOT MINGW)
    duckdb_extension_load(glue
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws-glue
            GIT_TAG 317342b6310341c9096c5e2e7601623ae3664b16
            APPLY_PATCHES
            SUBMODULES extension-ci-tools
            )
endif()
