add_library(test_sql_interval
                  OBJECT
                  test_interval.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_interval>
    PARENT_SCOPE)
