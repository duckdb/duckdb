include_directories(../../../third_party/dsdgen/include)

add_library_unity(test_sql_tpcds OBJECT test_tpcds.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_tpcds>
    PARENT_SCOPE)
