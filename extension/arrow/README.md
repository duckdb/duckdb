# DuckDB Arrow IPC roundtrip demo 
## Build instructions

make sure Arrow is installed (e.g. using `brew install apache-arrow` for mac)

set ARROW_PATH and and ARROW_INCLUDE env variables (e.g.: `/opt/homebrew/Cellar/apache-arrow/9.0.0_1(/include)`)

then run: `BUILD_ARROW=1 make` for release mode, or `DISABLE_SANITIZER=1 BUILD_ARROW=1 make debug` for debug.

Now build the node addon:
```
cd tools/nodejs
./configure
make <debug/release>
```

Currently you need to manually generate some test files, if you have a duckdb release installed this can be done with:  
```
duckdb -c "INSTALL parquet; LOAD parquet; INSTALL tpch; LOAD tpch; CALL DBGEN(sf=0.01); COPY lineitem to '/tmp/lineitem_sf0_01.parquet';"
duckdb -c "INSTALL parquet; LOAD parquet; INSTALL tpch; LOAD tpch; CALL DBGEN(sf=1); COPY lineitem to '/tmp/lineitem_sf1.parquet';"
```
Note that you can also build duckdb manually with the `BUILD_TPCH=1` flag to be be able to generate the above parquet files

Finally, if you're building in debug mode, in `tools/nodejs/test/arrow_ipc.test.js`, make sure the code loading the extension is switched to `debug`, to
make sure the extension with debug symbols is loaded.

Now to run tests, in the `tools/nodejs` dir:
```
npm test
```