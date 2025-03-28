add_library_unity(duckdb_common_multi_file OBJECT multi_file_list.cpp
                  multi_file_reader.cpp multi_file_reader_column_mapper.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_multi_file>
    PARENT_SCOPE)
