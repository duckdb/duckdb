add_subdirectory(art)
add_library(duckdb_index_execution OBJECT order_index.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_index_execution> PARENT_SCOPE)

