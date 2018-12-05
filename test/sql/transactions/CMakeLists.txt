
add_library(test_sql_transaction OBJECT test_concurrentappend.cpp test_concurrentdelete.cpp test_concurrentupdate.cpp test_schema_transactions.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_transaction> PARENT_SCOPE)
