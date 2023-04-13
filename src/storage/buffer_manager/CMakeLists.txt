add_library_unity(
  duckdb_storage_buffer_manager OBJECT buffer_manager.cpp
  standard_buffer_pool.cpp standard_buffer_manager.cpp cbuffer_manager.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_buffer_manager>
    PARENT_SCOPE)
