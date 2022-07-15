add_subdirectory(buffer)
add_subdirectory(checkpoint)
add_subdirectory(compression)
add_subdirectory(statistics)
add_subdirectory(table)

add_library_unity(
  duckdb_storage
  OBJECT
  arena_allocator.cpp
  buffer_manager.cpp
  checkpoint_manager.cpp
  block.cpp
  data_table.cpp
  index.cpp
  local_storage.cpp
  meta_block_reader.cpp
  meta_block_writer.cpp
  storage_manager.cpp
  table_index.cpp
  write_ahead_log.cpp
  single_file_block_manager.cpp
  storage_info.cpp
  storage_lock.cpp
  wal_replay.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage>
    PARENT_SCOPE)
