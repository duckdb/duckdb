add_library_unity(
  duckdb_optimizer_statistics_expr
  OBJECT
  propagate_aggregate.cpp
  propagate_between.cpp
  propagate_case.cpp
  propagate_cast.cpp
  propagate_comparison.cpp
  propagate_conjunction.cpp
  propagate_constant.cpp
  propagate_columnref.cpp
  propagate_function.cpp
  propagate_operator.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_optimizer_statistics_expr>
    PARENT_SCOPE)
