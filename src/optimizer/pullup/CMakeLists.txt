add_library_unity(
  duckdb_optimizer_pullup
  OBJECT
  pullup_filter.cpp
  pullup_projection.cpp
  pullup_from_left.cpp
  pullup_both_side.cpp
  pullup_set_operation.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_optimizer_pullup>
    PARENT_SCOPE)
