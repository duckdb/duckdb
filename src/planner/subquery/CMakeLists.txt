add_library_unity(
  duckdb_planner_subquery OBJECT flatten_dependent_join.cpp
  has_correlated_expressions.cpp rewrite_correlated_expressions.cpp
  rewrite_cte_scan.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_planner_subquery>
    PARENT_SCOPE)
