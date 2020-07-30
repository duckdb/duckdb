add_library_unity(test_sql_interquery_parallelism
                  OBJECT
                  test_concurrentappend.cpp
                  test_concurrentdelete.cpp
                  test_concurrent_dependencies.cpp
                  test_concurrentupdate.cpp
                  test_concurrent_sequence.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_interquery_parallelism>
    PARENT_SCOPE)
