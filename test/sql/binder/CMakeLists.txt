add_library_unity(test_sql_binder OBJECT test_case_insensitive_binding.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_binder>
    PARENT_SCOPE)
