add_library_unity(duckdb_transformer_expression
                  OBJECT
                  transform_bool_expr.cpp
                  transform_case.cpp
                  transform_cast.cpp
                  transform_coalesce.cpp
                  transform_columnref.cpp
                  transform_constant.cpp
                  transform_expression.cpp
                  transform_function.cpp
                  transform_is_null.cpp
                  transform_operator.cpp
                  transform_param_ref.cpp
                  transform_subquery.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_transformer_expression>
    PARENT_SCOPE)
