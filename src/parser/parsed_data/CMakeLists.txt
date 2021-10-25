add_library_unity(duckdb_parsed_data OBJECT alter_table_info.cpp
                  alter_sequence_info.cpp sample_options.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_parsed_data>
    PARENT_SCOPE)
