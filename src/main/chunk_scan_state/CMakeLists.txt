add_library_unity(duckdb_main_chunk_scan_state OBJECT query_result.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_chunk_scan_state>
    PARENT_SCOPE)
