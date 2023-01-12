add_subdirectory(arrow)
add_subdirectory(crypto)
add_subdirectory(enums)
add_subdirectory(operator)
add_subdirectory(progress_bar)
add_subdirectory(row_operations)
add_subdirectory(serializer)
add_subdirectory(sort)
add_subdirectory(types)
add_subdirectory(value_operations)
add_subdirectory(vector_operations)

if(${EXIT_TIME_DESTRUCTORS_WARNING})
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -Wno-exit-time-destructors")
endif()

add_library_unity(
  duckdb_common
  OBJECT
  allocator.cpp
  assert.cpp
  box_renderer.cpp
  compressed_file_system.cpp
  constants.cpp
  checksum.cpp
  cycle_counter.cpp
  exception.cpp
  exception_format_value.cpp
  field_writer.cpp
  file_buffer.cpp
  file_system.cpp
  fsst.cpp
  gzip_file_system.cpp
  hive_partitioning.cpp
  pipe_file_system.cpp
  local_file_system.cpp
  preserved_error.cpp
  printer.cpp
  radix_partitioning.cpp
  re2_regex.cpp
  random_engine.cpp
  serializer.cpp
  string_util.cpp
  symbols.cpp
  tree_renderer.cpp
  types.cpp
  virtual_file_system.cpp
  windows_util.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common>
    PARENT_SCOPE)

if(${BUILD_JEMALLOC_EXTENSION})
  include_directories(${PROJECT_SOURCE_DIR}/extension/jemalloc/include)
  add_definitions(-DBUILD_JEMALLOC_EXTENSION=${BUILD_JEMALLOC_EXTENSION})
endif()
