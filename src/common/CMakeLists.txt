if(NOT CLANG_TIDY)
  add_subdirectory(adbc)
endif()

add_subdirectory(arrow)
add_subdirectory(crypto)
add_subdirectory(enums)
add_subdirectory(exception)
add_subdirectory(operator)
add_subdirectory(progress_bar)
add_subdirectory(tree_renderer)
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

if(DEBUG_STACKTRACE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_DEBUG_STACKTRACE")
endif()

add_library_unity(
  duckdb_common
  OBJECT
  allocator.cpp
  assert.cpp
  bind_helpers.cpp
  box_renderer.cpp
  cgroups.cpp
  compressed_file_system.cpp
  constants.cpp
  checksum.cpp
  encryption_state.cpp
  exception.cpp
  exception_format_value.cpp
  extra_type_info.cpp
  file_buffer.cpp
  file_system.cpp
  filename_pattern.cpp
  fsst.cpp
  gzip_file_system.cpp
  hive_partitioning.cpp
  http_util.cpp
  pipe_file_system.cpp
  local_file_system.cpp
  multi_file_list.cpp
  multi_file_reader.cpp
  error_data.cpp
  printer.cpp
  radix_partitioning.cpp
  re2_regex.cpp
  random_engine.cpp
  string_util.cpp
  enum_util.cpp
  symbols.cpp
  render_tree.cpp
  tree_renderer.cpp
  types.cpp
  virtual_file_system.cpp
  windows_util.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common>
    PARENT_SCOPE)
