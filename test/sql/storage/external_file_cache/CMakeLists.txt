add_library_unity(test_sql_storage_external_file_cache OBJECT
                  test_external_file_cache.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_storage_external_file_cache>
    PARENT_SCOPE)
