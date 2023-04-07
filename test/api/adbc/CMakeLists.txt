add_library_unity(test_sql_adbc OBJECT test_adbc.cpp)

add_compile_definitions(
  DUCKDB_INSTALL_LIB="${CMAKE_BINARY_DIR}/src/libduckdb.dylib")

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_adbc>
    PARENT_SCOPE)
