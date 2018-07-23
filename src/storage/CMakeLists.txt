
add_library(duckdb_storage OBJECT data_table.cpp storage_manager.cpp data_column.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage> PARENT_SCOPE)
