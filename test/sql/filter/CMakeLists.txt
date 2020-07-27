add_library_unity(test_sql_filter
                  OBJECT
                  test_alias_filter.cpp
                  test_constant_comparisons.cpp
                  test_illegal_filters.cpp
                  test_obsolete_filters.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_filter>
    PARENT_SCOPE)
