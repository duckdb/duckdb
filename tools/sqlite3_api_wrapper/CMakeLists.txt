include_directories(include)

add_library(sqlite3_api_wrapper SHARED sqlite3_api_wrapper.cpp)
target_link_libraries(sqlite3_api_wrapper duckdb)

include_directories(../../third_party/catch)

add_executable(test_sqlite3_api_wrapper test_sqlite3_api_wrapper.cpp)
target_link_libraries(test_sqlite3_api_wrapper sqlite3_api_wrapper duckdb)
