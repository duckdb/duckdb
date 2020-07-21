if(NOT WIN32)
  add_library_unity(test_api
                    OBJECT
                    test_api.cpp
                    test_results.cpp
                    test_read_only.cpp
                    test_prepared_api.cpp
                    test_table_info.cpp
                    test_appender_api.cpp
                    test_relation_api.cpp
                    test_query_profiler.cpp
                    test_dbdir.cpp)
else()
  add_library_unity(test_api
                    OBJECT
                    test_api.cpp
                    test_results.cpp
                    test_prepared_api.cpp
                    test_table_info.cpp
                    test_appender_api.cpp
                    test_relation_api.cpp
                    test_query_profiler.cpp
                    test_dbdir.cpp)
endif()

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_api>
    PARENT_SCOPE)
