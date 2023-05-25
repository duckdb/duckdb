add_library_unity(duckdb_main_query_result OBJECT materialized_query_result.cpp
                  stream_query_result.cpp pending_query_result.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_query_result>
    PARENT_SCOPE)
