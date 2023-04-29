add_subdirectory(bit)
add_subdirectory(blob)
add_subdirectory(date)
add_subdirectory(enum)
add_subdirectory(generic)
add_subdirectory(list)
add_subdirectory(map)
add_subdirectory(struct)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    PARENT_SCOPE)
