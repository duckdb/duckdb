add_subdirectory(expression)
add_subdirectory(query_node)
add_subdirectory(statement)
add_subdirectory(tableref)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    PARENT_SCOPE)
