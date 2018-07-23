
add_library(duckdb_common_types OBJECT date.cpp value.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_types> PARENT_SCOPE)
