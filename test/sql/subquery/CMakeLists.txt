add_library_unity(test_subquery
                  OBJECT
                  test_any_all.cpp
                  test_correlated_subquery.cpp
                  test_subquery.cpp
                  test_lateral.cpp
                  test_scalar_subquery.cpp
                  test_update_subquery.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_subquery>
    PARENT_SCOPE)
