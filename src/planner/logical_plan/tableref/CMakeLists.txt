add_library_unity(duckdb_logical_plan_tableref
                  OBJECT
                  plan_basetableref.cpp
                  plan_crossproductref.cpp
                  plan_dummytableref.cpp
                  plan_expressionlistref.cpp
                  plan_joinref.cpp
                  plan_subqueryref.cpp
                  plan_table_function.cpp
                  plan_cteref.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_logical_plan_tableref>
    PARENT_SCOPE)
