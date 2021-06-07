add_subdirectory(relation)

set(DUCKDB_MAIN_FILES
    appender.cpp
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

if(NOT CLANG_TIDY)
  set(DUCKDB_MAIN_FILES ${DUCKDB_MAIN_FILES} duckdb-c.cpp)
endif()

add_library_unity(duckdb_main OBJECT ${DUCKDB_MAIN_FILES})
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main>
    PARENT_SCOPE)
