
add_library(test_sql_tpce OBJECT tpce.cpp)
    set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS : test_sql_tpce>
            PARENT_SCOPE)
