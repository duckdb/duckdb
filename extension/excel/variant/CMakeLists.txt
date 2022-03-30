include_directories(include)

add_library(variant OBJECT variant_value.cpp)

set(VARIANT_OBJECT_FILES
    ${VARIANT_OBJECT_FILES} $<TARGET_OBJECTS:variant>
    PARENT_SCOPE)

disable_target_warnings(variant)
