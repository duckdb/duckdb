duckdb_extension_load(quack
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-quack
    GIT_TAG fa3f82c53cf587838d55efbd24f31b0c055684a9
    SUBMODULES extension-ci-tools
    APPLY_PATCHES
)
