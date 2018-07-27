
add_library(duckdb_common_types OBJECT data_chunk.cpp date.cpp hash.cpp operators.cpp value.cpp vector.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_types> PARENT_SCOPE)
