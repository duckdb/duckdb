if(${EXIT_TIME_DESTRUCTORS_WARNING})
    set(CMAKE_CXX_FLAGS_DEBUG
            "${CMAKE_CXX_FLAGS_DEBUG} -Wno-exit-time-destructors")
endif()

add_library_unity(
  duckdb_common_types_row
  OBJECT
  row_data_collection.cpp
  row_data_collection_scanner.cpp
  row_layout.cpp
  tuple_data_collection.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_types_row>
    PARENT_SCOPE)