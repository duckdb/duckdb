add_library_unity(duckdb_main_buffered_data OBJECT simple_buffered_data.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_buffered_data>
    PARENT_SCOPE)
