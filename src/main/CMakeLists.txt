if(NOT CLANG_TIDY)
  add_subdirectory(capi)
endif()
add_subdirectory(extension)
add_subdirectory(relation)
add_subdirectory(settings)
add_subdirectory(chunk_scan_state)

if(FORCE_QUERY_LOG)
  add_definitions(-DDUCKDB_FORCE_QUERY_LOG="\""${FORCE_QUERY_LOG}"\"")
endif()

add_library_unity(
  duckdb_main
  OBJECT
  appender.cpp
  attached_database.cpp
  client_context_file_opener.cpp
  client_context.cpp
  client_data.cpp
  client_verify.cpp
  chunk_scan_state.cpp
  config.cpp
  connection.cpp
  database.cpp
  database_path_and_type.cpp
  database_manager.cpp
  db_instance_cache.cpp
  error_manager.cpp
  extension.cpp
  materialized_query_result.cpp
  pending_query_result.cpp
  prepared_statement.cpp
  prepared_statement_data.cpp
  relation.cpp
  query_profiler.cpp
  query_result.cpp
  stream_query_result.cpp
  valid_checker.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main>
    PARENT_SCOPE)
