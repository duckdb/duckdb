add_library_unity(test_sql_parallelism OBJECT test_simple_parallelism.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_parallelism>
    PARENT_SCOPE)
