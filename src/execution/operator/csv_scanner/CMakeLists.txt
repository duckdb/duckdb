add_subdirectory(sniffer)

add_library_unity(
  duckdb_operator_csv_scanner
  OBJECT
  base_csv_reader.cpp
  buffered_csv_reader.cpp
  csv_buffer.cpp
  csv_buffer_manager.cpp
  csv_file_handle.cpp
  csv_reader_options.cpp
  csv_state_machine.cpp
  csv_state_machine_cache.cpp
  parallel_csv_reader.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_csv_scanner>
    PARENT_SCOPE)
