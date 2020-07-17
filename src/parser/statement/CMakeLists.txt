add_library(duckdb_statement OBJECT select_statement.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_statement>
    PARENT_SCOPE)
