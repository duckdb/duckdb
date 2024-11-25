add_library_unity(
  duckdb_func_window
  OBJECT
  window_boundaries_state.cpp
  window_collection.cpp
  window_cumedist_function.cpp
  window_executor.cpp
  window_rank_function.cpp
  window_segment_tree.cpp
  window_shared_expressions.cpp
  window_value_function.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_window>
    PARENT_SCOPE)
