add_library_unity(test_sql_transaction_types
		OBJECT
		test_interval_transaction.cpp)
set(ALL_OBJECT_FILES
${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_transaction_types>
PARENT_SCOPE)
