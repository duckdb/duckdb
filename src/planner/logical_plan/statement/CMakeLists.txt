add_library_unity(duckdb_logical_plan_statement
                  OBJECT
                  plan_copy.cpp
                  plan_create.cpp
                  plan_delete.cpp
                  plan_execute.cpp
                  plan_explain.cpp
                  plan_insert.cpp
                  plan_select.cpp
                  plan_simple.cpp
                  plan_update.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_logical_plan_statement>
    PARENT_SCOPE)
