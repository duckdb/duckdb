add_subdirectory(filter)
add_subdirectory(function)
add_subdirectory(index)
add_subdirectory(parallelism)
add_subdirectory(storage)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    PARENT_SCOPE)
