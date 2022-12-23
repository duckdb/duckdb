add_subdirectory(capi)
add_subdirectory(udf_function)

set(TEST_API_OBJECTS
    test_api.cpp
    test_config.cpp
    test_custom_allocator.cpp
    test_results.cpp
    test_reset.cpp
    test_get_table_names.cpp
    test_prepared_api.cpp
    test_table_info.cpp
    test_appender_api.cpp
    test_pending_query.cpp
    test_plan_serialization.cpp
    test_plan_serialization_across_versions.cpp
    test_relation_api.cpp
    test_query_profiler.cpp
    test_dbdir.cpp
    test_progress_bar.cpp
    test_threads.cpp
    test_windows_header_compatibility.cpp)

if(NOT WIN32)
  set(TEST_API_OBJECTS ${TEST_API_OBJECTS} test_read_only.cpp)
endif()

if(${BUILD_TPCH_EXTENSION} AND NOT ${DISABLE_BUILTIN_EXTENSIONS})
  include_directories(../../extension/tpch/include)
  set(TEST_API_OBJECTS ${TEST_API_OBJECTS} test_tpch_with_relations.cpp
                       serialized_plans/test_plan_serialization_bwc.cpp)
endif()

add_library_unity(test_api OBJECT ${TEST_API_OBJECTS})
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_api>
    PARENT_SCOPE)
