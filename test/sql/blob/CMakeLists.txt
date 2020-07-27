add_library_unity(test_sql_blob OBJECT test_blob.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_blob>
    PARENT_SCOPE)
