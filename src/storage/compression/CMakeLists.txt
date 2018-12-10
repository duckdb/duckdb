add_library(duckdb_storage_compression OBJECT dictionary_encoder.cpp base_encoder.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_compression> PARENT_SCOPE)
