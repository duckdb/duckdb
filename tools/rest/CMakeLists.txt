include_directories(.)
include_directories(../../third_party/dbgen/include)

add_executable(duckdb_rest_server server.cpp)
target_link_libraries(duckdb_rest_server duckdb_static Threads::Threads)

add_executable(duckdb_dbgen dbgen.cpp)
target_link_libraries(duckdb_dbgen dbgen duckdb_static)
