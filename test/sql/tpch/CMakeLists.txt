add_library_unity(test_sql_tpch
                  OBJECT
                  test_tpch.cpp
                  test_tpch_with_relations.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_tpch>
    PARENT_SCOPE)
