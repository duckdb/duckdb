add_subdirectory(common)
add_subdirectory(i18n)
add_subdirectory(stubdata)

set(ICU_LIBRARY_FILES
    ${ICU_LIBRARY_FILES}
    PARENT_SCOPE)
