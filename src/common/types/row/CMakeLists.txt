if(${EXIT_TIME_DESTRUCTORS_WARNING})
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -Wno-exit-time-destructors")
endif()

add_library_unity(
  duckdb_common_types_row
  OBJECT
  partitioned_tuple_data.cpp
  tuple_data_allocator.cpp
  tuple_data_collection.cpp
  tuple_data_iterator.cpp
  tuple_data_layout.cpp
  tuple_data_scatter_gather.cpp
  tuple_data_segment.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_types_row>
    PARENT_SCOPE)
