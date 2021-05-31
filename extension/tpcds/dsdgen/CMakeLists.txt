add_subdirectory(dsdgen-c)

add_library(dsdgen STATIC dsdgen.cpp append_info-c.cpp dsdgen_helpers.cpp)

set(DSDGEN_OBJECT_FILES
    ${DSDGEN_OBJECT_FILES} $<TARGET_OBJECTS:dsdgen>
    PARENT_SCOPE)

disable_target_warnings(dsdgen)
