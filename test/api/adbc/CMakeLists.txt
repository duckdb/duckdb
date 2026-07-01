include_directories(${PROJECT_SOURCE_DIR}/third_party/nanoarrow)
add_library_unity(test_sql_adbc OBJECT test_adbc.cpp driver_manager.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_adbc>
    PARENT_SCOPE)
