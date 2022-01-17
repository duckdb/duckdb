include_directories(include)

add_library(yyjson OBJECT yyjson.cpp)
set(YYJSON_OBJECT_FILES
    ${YYJSON_OBJECT_FILES} $<TARGET_OBJECTS:yyjson>
    PARENT_SCOPE)

disable_target_warnings(yyjson)
