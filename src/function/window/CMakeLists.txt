add_library_unity(
  duckdb_func_window OBJECT window_boundaries_state.cpp window_collection.cpp
  window_executor.cpp window_segment_tree.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_window>
    PARENT_SCOPE)
