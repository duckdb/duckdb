include_directories(.)

add_executable(duckdb_rest_server server.cpp)
target_link_libraries(duckdb_rest_server duckdb_static)
