
add_library(test_common OBJECT test_date.cpp test_hyperloglog.cpp test_serializer.cpp test_sort.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_common> PARENT_SCOPE)
