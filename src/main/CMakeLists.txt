add_library(duckdb_main OBJECT duckdb-c.cpp duckdb.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main> PARENT_SCOPE)
