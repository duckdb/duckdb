add_library_unity(
  duckdb_storage_buffer_manager
  OBJECT
  buffer_manager.cpp
  virtual_buffer_manager.cpp
  block_manager.cpp
  block_handle.cpp
  cbuffer_manager.cpp
  buffer_pool_reservation.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_buffer_manager>
    PARENT_SCOPE)
