include_directories(.)

include_directories(../../third_party/httplib)

add_executable(duckdb_rest_server server.cpp)

if(${BUILD_SUN})
  set(LINK_EXTRA -lsocket)
endif()

add_extension_definitions()

target_link_libraries(duckdb_rest_server duckdb_static ${LINK_EXTRA})
link_threads(duckdb_rest_server)
