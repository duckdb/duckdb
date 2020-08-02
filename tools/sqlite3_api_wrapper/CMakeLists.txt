include_directories(include)


if(${BUILD_ICU_EXTENSION})
  add_definitions(-DBUILD_ICU_EXTENSION=${BUILD_ICU_EXTENSION})
endif()

if(${BUILD_PARQUET_EXTENSION})
  add_definitions(-DBUILD_PARQUET_EXTENSION=${BUILD_PARQUET_EXTENSION})
endif()

add_definitions(-DDUCKDB_SOURCE_ID="\""${GIT_COMMIT_HASH}"\"")

add_library(sqlite3_api_wrapper SHARED sqlite3_api_wrapper.cpp printf.c)
target_link_libraries(sqlite3_api_wrapper duckdb Threads::Threads)

add_library(sqlite3_api_wrapper_static STATIC sqlite3_api_wrapper.cpp printf.c)
target_link_libraries(sqlite3_api_wrapper_static duckdb_static Threads::Threads)

if(${BUILD_PARQUET_EXTENSION})
	include_directories(../../extension/parquet/include)
    target_link_libraries(sqlite3_api_wrapper parquet_extension)
    target_link_libraries(sqlite3_api_wrapper_static parquet_extension)
endif()

if(${BUILD_ICU_EXTENSION})
	include_directories(../../extension/icu/include)
    target_link_libraries(sqlite3_api_wrapper icu_extension)
    target_link_libraries(sqlite3_api_wrapper_static icu_extension)
endif()

include_directories(../../third_party/catch)

add_executable(test_sqlite3_api_wrapper test_sqlite3_api_wrapper.cpp)
target_link_libraries(test_sqlite3_api_wrapper
                      sqlite3_api_wrapper)



# For testing only: compile a version of test_sqlite3_api_wrapper that is bound
# to SQLite: this should have the same behavior as binding to DuckDB
# add_executable(test_sqlite3_api_wrapper_sqlite test_sqlite3_api_wrapper.cpp
# ../../third_party/sqlite/sqlite3.c)
# target_link_libraries(test_sqlite3_api_wrapper_sqlite sqlite3_api_wrapper
# Threads::Threads) target_compile_definitions(test_sqlite3_api_wrapper_sqlite
# PUBLIC -DSQLITE_TEST)
