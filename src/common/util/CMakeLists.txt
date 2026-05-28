add_library_unity(duckdb_common_util OBJECT util_parsed_expression.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_util>
    PARENT_SCOPE)
