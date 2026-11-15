if (NOT MINGW)
    duckdb_extension_load(glue
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws-glue
            GIT_TAG 07373b8a002f833e25f1e1a8228f4c1d7e3f1898
            APPLY_PATCHES
            SUBMODULES extension-ci-tools
            )
endif()
