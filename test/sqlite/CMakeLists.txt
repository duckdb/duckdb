add_library_unity(test_sqlite
                  OBJECT
                  md5.cpp
                  test_sqllogictest.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sqlite>
    PARENT_SCOPE)
