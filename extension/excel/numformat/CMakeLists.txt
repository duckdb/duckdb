include_directories(include)

add_library(numformat OBJECT nf_calendar.cpp nf_localedata.cpp nf_zformat.cpp)

set(NUMFORMAT_OBJECT_FILES
    ${NUMFORMAT_OBJECT_FILES} $<TARGET_OBJECTS:numformat>
    PARENT_SCOPE)

disable_target_warnings(numformat)
