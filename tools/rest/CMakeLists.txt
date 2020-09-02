include_directories(.)

add_executable(duckdb_rest_server server.cpp)

if(${BUILD_SUN})
  set(LINK_EXTRA -lsocket)
endif()

target_link_libraries(duckdb_rest_server duckdb_static Threads::Threads
                      ${LINK_EXTRA})
