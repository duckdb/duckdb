add_library_unity(duckdb_storage_buffer OBJECT system_buffer_handle.cpp
                  cbuffer_handle.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_buffer>
    PARENT_SCOPE)
