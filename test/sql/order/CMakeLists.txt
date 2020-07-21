add_library_unity(test_sql_order
        OBJECT
        test_order_same_value.cpp)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_order>
        PARENT_SCOPE)
