add_library_unity(
  odbc_common
  OBJECT
  duckdb_odbc.cpp
  handle_functions.cpp
  odbc_diagnostic.cpp
  odbc_fetch.cpp
  odbc_interval.cpp
  odbc_utils.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_common>
    PARENT_SCOPE)
