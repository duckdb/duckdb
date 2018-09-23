
add_library(duckdb_storage OBJECT data_table.cpp storage_chunk.cpp storage_manager.cpp write_ahead_log.cpp )
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage> PARENT_SCOPE)
