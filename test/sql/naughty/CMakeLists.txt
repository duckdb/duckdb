add_library_unity(test_sql_naughty OBJECT test_naughty_strings.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_naughty>
    PARENT_SCOPE)
