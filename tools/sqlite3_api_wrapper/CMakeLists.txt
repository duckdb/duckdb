
include_directories(include)

add_library(sqlite3_api_wrapper SHARED sqlite3_api_wrapper.c)
target_link_libraries(sqlite3_api_wrapper duckdb)
