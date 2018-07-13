add_library(duckdb_parser OBJECT parser.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_parser> PARENT_SCOPE)
