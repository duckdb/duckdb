add_library(duckdb_common OBJECT internal_types.cpp printable.cpp sql_node_visitor.cpp string_util.cpp value.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common> PARENT_SCOPE)
