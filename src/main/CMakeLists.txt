if(NOT CLANG_TIDY)
  add_subdirectory(capi)
endif()
add_subdirectory(extension)
add_subdirectory(relation)

if(FORCE_QUERY_LOG)
  add_definitions(-DDUCKDB_FORCE_QUERY_LOG="\""${FORCE_QUERY_LOG}"\"")
endif()

add_library_unity(
  duckdb_main
  OBJECT
  appender.cpp
  client_context_file_opener.cpp
  client_context.cpp
  config.cpp
  connection.cpp
  database.cpp
  materialized_query_result.cpp
  prepared_statement.cpp
  prepared_statement_data.cpp
  relation.cpp
  query_profiler.cpp
  query_result.cpp
  stream_query_result.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main>
    PARENT_SCOPE)
