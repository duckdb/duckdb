add_subdirectory(sqlsmith)

set(SQLSMITH_OBJECT_FILES
    ${SQLSMITH_OBJECT_FILES} $<TARGET_OBJECTS:dbgen>
    PARENT_SCOPE)
