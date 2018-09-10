
add_library(duckdb_common_types OBJECT chunk_collection.cpp data_chunk.cpp date.cpp hash.cpp operators.cpp string_heap.cpp value.cpp vector.cpp vector_aggregates.cpp vector_generators.cpp vector_helpers.cpp vector_operations.cpp vector_scatter.cpp statistics.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_types> PARENT_SCOPE)
