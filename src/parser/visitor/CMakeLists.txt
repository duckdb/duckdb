add_library_unity(duckdb_parse_tree_visitor OBJECT parse_tree_visitor.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_parse_tree_visitor>
    PARENT_SCOPE)
