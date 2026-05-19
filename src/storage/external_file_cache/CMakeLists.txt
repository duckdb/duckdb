add_library_unity(
  duckdb_external_file_cache
  OBJECT
  caching_file_system.cpp
  caching_file_system_wrapper.cpp
  external_file_cache.cpp
  external_file_cache_block.cpp
  external_file_cache_util.cpp
  file_buffer_handle_group.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_external_file_cache>
    PARENT_SCOPE)
