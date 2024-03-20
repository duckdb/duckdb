
It's required to have a JDK installed to build.
Make sure the `JAVA_HOME` environment variable is set.

If you are on a Mac and install `openjdk` via `brew` then additionally, it's required to set:
```
export JAVA_AWT_LIBRARY=$JAVA_HOME/libexec/openjdk.jdk/Contents/Home/lib
```
because the [`FindJNI.cmake`](https://cmake.org/cmake/help/latest/module/FindJNI.html) module doesn't look there for the `awt` library.

### Development

Be sure to build with `DISABLE_SANITIZER=1` and `BUILD_JDBC=1` enabled

This will produce two jars in the build folder:
`build/<build_type>/tools/jdbc/duckdb_jdbc.jar`
`build/<build_type>/tools/jdbc/duckdb_jdbc_tests.jar`

The tests can be ran using this command (taking a `debug` build for example)
```
java -cp "build/debug/tools/jdbc/duckdb_jdbc_tests.jar:build/debug/tools/jdbc/duckdb_jdbc.jar" org/duckdb/TestDuckDBJDBC
```

This optionally takes an argument to only run a single test, for example:
```
java -cp "build/debug/tools/jdbc/duckdb_jdbc_tests.jar:build/debug/tools/jdbc/duckdb_jdbc.jar"  org/duckdb/TestDuckDBJDBC test_valid_but_local_config_throws_exception
```
