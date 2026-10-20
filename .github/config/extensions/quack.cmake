duckdb_extension_load(quack
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-quack
    GIT_TAG e7a9be3f8c1c3be5946e534c96f15b56b81dc026
    SUBMODULES extension-ci-tools
    APPLY_PATCHES
)
