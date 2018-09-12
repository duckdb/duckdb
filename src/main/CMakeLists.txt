add_library(duckdb_main OBJECT connection.cpp database.cpp duckdb-c.cpp query_profiler.cpp result.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main> PARENT_SCOPE)
