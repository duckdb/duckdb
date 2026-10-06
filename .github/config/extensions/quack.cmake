duckdb_extension_load(quack
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-quack
    GIT_TAG ef2534bc2bc64465d7f67b4df64d269290fa9308
    SUBMODULES extension-ci-tools
    APPLY_PATCHES
)
