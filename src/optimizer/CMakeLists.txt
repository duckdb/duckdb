add_library(duckdb_optimizer OBJECT rewriter.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_optimizer> PARENT_SCOPE)
