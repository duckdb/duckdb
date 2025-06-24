add_library_unity(duckdb_csv_buffer_manager OBJECT csv_buffer.cpp
                  csv_buffer_manager.cpp csv_file_handle.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_csv_buffer_manager>
    PARENT_SCOPE)
