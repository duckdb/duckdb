# Updating jemalloc

Clone [this](https://github.com/jemalloc/jemalloc).

For convenience:
```sh
export DUCKDB_DIR=<duckdb_dir>
```

Copy jemalloc source files:
```sh
cd <jemalloc_dir>
./configure --with-jemalloc-prefix="duckdb_je_" --with-private-namespace="duckdb_je_"
cp -r src/* $DUCKDB_DIR/extension/jemalloc/jemalloc/src/
cp -r include/* $DUCKDB_DIR/extension/jemalloc/jemalloc/include/
cp COPYING $DUCKDB_DIR/extension/jemalloc/jemalloc/LICENSE
```

Remove junk:
```sh
cd $DUCKDB_DIR/extension/jemalloc/jemalloc
find . -name "*.in" -type f -delete
find . -name "*.sh" -type f -delete
find . -name "*.awk" -type f -delete
find . -name "*.txt" -type f -delete
find . -name "*.py" -type f -delete
```

Restore these
```sh
git checkout -- include/jemalloc/internal/jemalloc_internal_defs.h CMakeLists.txt
```
