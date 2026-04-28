add_library_unity(test_sql_adbc OBJECT test_adbc.cpp driver_manager.cpp)

set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_adbc>
    PARENT_SCOPE)
