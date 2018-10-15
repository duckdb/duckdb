add_library(duckdb_parser_tableref OBJECT subqueryref.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_parser_tableref> PARENT_SCOPE)
