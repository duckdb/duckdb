add_library_unity(test_pivot OBJECT multi_statement_parsing.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_pivot>
    PARENT_SCOPE)
