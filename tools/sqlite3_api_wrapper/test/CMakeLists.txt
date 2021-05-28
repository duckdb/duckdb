set(CUR_DIR
    ${CMAKE_CURRENT_SOURCE_DIR})

set(SQLITE_TEST_FILES
    ${CUR_DIR}/test_sqlite3_api_wrapper.cpp
    ${CUR_DIR}/test_sqlite3_udf_api_wrapper.cpp
    PARENT_SCOPE)
