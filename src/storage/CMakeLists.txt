add_library_unity(duckdb_storage OBJECT checkpoint_manager.cpp column_statistics.cpp block.cpp data_table.cpp meta_block_reader.cpp meta_block_writer.cpp unique_index.cpp storage_chunk.cpp storage_manager.cpp write_ahead_log.cpp single_file_block_manager.cpp storage_info.cpp wal_replay.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage> PARENT_SCOPE)
