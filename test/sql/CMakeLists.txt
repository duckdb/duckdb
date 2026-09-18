add_subdirectory(filter)
add_subdirectory(function)
add_subdirectory(index)
add_subdirectory(parallelism)
add_subdirectory(pivot)
add_subdirectory(storage)

set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES}
    PARENT_SCOPE)
