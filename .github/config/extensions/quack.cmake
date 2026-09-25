duckdb_extension_load(quack
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-quack
    GIT_TAG 9b9cfaf0d6c2b89590a7512eb2473a2008e859ce
    SUBMODULES extension-ci-tools
    APPLY_PATCHES
)
