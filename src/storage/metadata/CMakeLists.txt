add_library_unity(duckdb_storage_metadata OBJECT metadata_manager.cpp
                  metadata_reader.cpp metadata_writer.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_metadata>
    PARENT_SCOPE)
