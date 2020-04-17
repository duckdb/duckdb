add_library_unity(test_parquet OBJECT test_parquet.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_parquet>
    PARENT_SCOPE)
