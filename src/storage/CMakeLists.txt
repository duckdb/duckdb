add_subdirectory(compression)

add_library(duckdb_storage OBJECT column_statistics.cpp data_table.cpp unique_index.cpp storage_chunk.cpp storage_manager.cpp write_ahead_log.cpp )
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage> PARENT_SCOPE)
