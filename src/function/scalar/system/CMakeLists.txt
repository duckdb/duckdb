add_library_unity(duckdb_func_system OBJECT aggregate_export.cpp write_log.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_system>
    PARENT_SCOPE)
