include_directories(../include)
include_directories(../../../third_party/catch/)
include_directories(${ODBC_INCLUDE_DIRS})

add_executable(
  test_odbc
  common.cpp
  tests/alter.cpp
  tests/basic_usage.cpp
  tests/bind_col.cpp
  tests/bools_as_char.cpp
  tests/catalog_functions.cpp
  tests/col_attribute.cpp
  tests/connect.cpp
  tests/diagnostics.cpp
  tests/select.cpp
  tests/row_wise_fetching.cpp
  tests/set_attr.cpp
  tests/cte.cpp
  tests/cursor_commit.cpp
  tests/declare_fetch_block.cpp
  tests/data_execution.cpp
  tests/multicolumn_param_bind.cpp
  tests/numeric.cpp
  tests/quotes.cpp
  tests/result_conversion.cpp)

target_link_libraries(test_odbc duckdb_odbc ODBC::ODBC)
