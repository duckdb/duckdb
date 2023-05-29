add_subdirectory(aggregate)
add_subdirectory(filter)
add_subdirectory(helper)
add_subdirectory(join)
add_subdirectory(order)
add_subdirectory(persistent)
add_subdirectory(projection)
add_subdirectory(scan)
add_subdirectory(schema)
add_subdirectory(set)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    PARENT_SCOPE)
