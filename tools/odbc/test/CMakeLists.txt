include_directories(../include)
include_directories(../../../third_party/catch/)
include_directories(${ODBC_INCLUDE_DIRS})

add_executable(test_odbc test_odbc.cpp)
target_link_libraries(test_odbc duckdb_odbc ODBC::ODBC)
