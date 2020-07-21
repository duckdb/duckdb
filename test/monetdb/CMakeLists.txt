add_library_unity(test_monetdb
                  OBJECT
                  test_leftjoin_bug.cpp
                  test_many_views.cpp
                  test_updates_with_correlated_subselect.cpp
                  test_boolean_logic.cpp
                  test_round_bug.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_monetdb>
    PARENT_SCOPE)
