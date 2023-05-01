add_subdirectory(column)
add_subdirectory(row)

if(${EXIT_TIME_DESTRUCTORS_WARNING})
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -Wno-exit-time-destructors")
endif()

add_library_unity(
  duckdb_common_types
  OBJECT
  batched_data_collection.cpp
  bit.cpp
  blob.cpp
  cast_helpers.cpp
  chunk_collection.cpp
  conflict_manager.cpp
  conflict_info.cpp
  data_chunk.cpp
  date.cpp
  decimal.cpp
  hash.cpp
  hugeint.cpp
  uuid.cpp
  hyperloglog.cpp
  interval.cpp
  list_segment.cpp
  selection_vector.cpp
  string_heap.cpp
  string_type.cpp
  timestamp.cpp
  time.cpp
  validity_mask.cpp
  value.cpp
  vector_buffer.cpp
  vector.cpp
  vector_cache.cpp
  vector_constants.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_types>
    PARENT_SCOPE)
