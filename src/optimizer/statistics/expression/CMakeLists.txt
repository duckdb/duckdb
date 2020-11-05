add_library_unity(duckdb_optimizer_statistics_expr
   OBJECT
   propagate_cast.cpp
   propagate_constant.cpp
   propagate_columnref.cpp
   propagate_function.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
   $<TARGET_OBJECTS:duckdb_optimizer_statistics_expr> PARENT_SCOPE)
