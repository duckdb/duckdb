add_library_unity(test_sql_window OBJECT test_window_interrupt.cpp)
set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_window>
    PARENT_SCOPE)
