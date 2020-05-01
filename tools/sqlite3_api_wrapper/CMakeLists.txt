include_directories(include)

add_definitions(-DDUCKDB_SOURCE_ID="\""${GIT_COMMIT_HASH}"\"")

add_library(sqlite3_api_wrapper SHARED sqlite3_api_wrapper.cpp printf.c)
target_link_libraries(sqlite3_api_wrapper duckdb)

add_library(sqlite3_api_wrapper_static STATIC sqlite3_api_wrapper.cpp printf.c)
target_link_libraries(sqlite3_api_wrapper_static duckdb_static)


include_directories(../../third_party/catch)

add_executable(test_sqlite3_api_wrapper test_sqlite3_api_wrapper.cpp)
target_link_libraries(test_sqlite3_api_wrapper
                      sqlite3_api_wrapper
                      duckdb
                      Threads::Threads)

# For testing only: compile a version of test_sqlite3_api_wrapper that is bound
# to SQLite: this should have the same behavior as binding to DuckDB
# add_executable(test_sqlite3_api_wrapper_sqlite test_sqlite3_api_wrapper.cpp
# ../../third_party/sqlite/sqlite3.c)
# target_link_libraries(test_sqlite3_api_wrapper_sqlite sqlite3_api_wrapper
# Threads::Threads) target_compile_definitions(test_sqlite3_api_wrapper_sqlite
# PUBLIC -DSQLITE_TEST)
