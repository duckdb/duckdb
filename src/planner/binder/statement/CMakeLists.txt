add_library_unity(duckdb_bind_statement
                  OBJECT
                  bind_copy.cpp
                  bind_create.cpp
                  bind_create_table.cpp
                  bind_delete.cpp
                  bind_drop.cpp
                  bind_execute.cpp
                  bind_explain.cpp
                  bind_insert.cpp
                  bind_relation.cpp
                  bind_select.cpp
                  bind_simple.cpp
                  bind_update.cpp
                  bind_vacuum.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_bind_statement>
    PARENT_SCOPE)
