duckdb_extension_load(quack
    LOAD_TESTS
    GIT_URL https://github.com/duckdb/duckdb-quack
    GIT_TAG 02c9d4a085d7b4f4ad7846aefca8c8c1c78c2bd2
    APPLY_PATCHES
    SUBMODULES extension-ci-tools
)
