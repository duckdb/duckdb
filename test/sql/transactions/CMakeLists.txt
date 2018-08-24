
add_library(test_sql_transaction OBJECT concurrentreadwrite.cpp schema_transactions.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_transaction> PARENT_SCOPE)
