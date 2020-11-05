add_library_unity(duckdb_optimizer_statistics_op
   OBJECT
   propagate_filter.cpp
   propagate_get.cpp
   propagate_projection.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
   $<TARGET_OBJECTS:duckdb_optimizer_statistics_op> PARENT_SCOPE)
