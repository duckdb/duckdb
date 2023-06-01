include_directories(../include)
include_directories(../../../third_party/catch/)
include_directories(${ODBC_INCLUDE_DIRS})

add_executable(
  test_odbc common.cpp tests/basic_usage.cpp tests/alter.cpp tests/bindcol.cpp
            tests/boolsaschar.cpp tests/connect.cpp tests/diagnostics.cpp)
target_link_libraries(test_odbc duckdb_odbc ODBC::ODBC)
