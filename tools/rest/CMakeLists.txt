include_directories(.)
include_directories(../../third_party/dbgen/include)

add_definitions(-DDUCKDB_SOURCE_ID="\""${GIT_COMMIT_HASH}"\"")

add_executable(duckdb_rest_server server.cpp)

if(${BUILD_SUN})
  set(LINK_EXTRA -lsocket)
endif()

target_link_libraries(duckdb_rest_server duckdb_static Threads::Threads ${LINK_EXTRA})

add_executable(duckdb_dbgen dbgen.cpp)
target_link_libraries(duckdb_dbgen dbgen duckdb_static)
