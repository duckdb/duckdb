add_library(test_sql_create OBJECT test_ifnotexists.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_create>
    PARENT_SCOPE)
