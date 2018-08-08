
add_library(test_sqlite OBJECT md5.cpp sqllogictest.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sqlite> PARENT_SCOPE)
