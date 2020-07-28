add_library_unity(test_sql_transaction
                  OBJECT
                  test_big_transaction_abort.cpp
                  test_multiple_versions.cpp
                  test_sequence.cpp
                  test_schema_transactions.cpp
                  test_transaction_local_index.cpp
                  test_transaction_local.cpp
                  test_null_versions.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_transaction>
    PARENT_SCOPE)
