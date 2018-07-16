add_library(duckdb_common OBJECT internal_types.cpp printable.cpp string_util.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common> PARENT_SCOPE)
