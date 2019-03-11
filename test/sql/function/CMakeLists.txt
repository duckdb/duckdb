
add_library(test_sql_function OBJECT test_concat.cpp test_substring.cpp test_table_function.cpp test_round.cpp test_stringfunctions.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_function> PARENT_SCOPE)
