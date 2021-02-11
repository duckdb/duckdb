add_library_unity(
  duckdb_expression_executor
  OBJECT
  execute_between.cpp
  execute_case.cpp
  execute_cast.cpp
  execute_comparison.cpp
  execute_conjunction.cpp
  execute_constant.cpp
  execute_function.cpp
  execute_operator.cpp
  execute_parameter.cpp
  execute_reference.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_expression_executor>
    PARENT_SCOPE)
