add_library_unity(duckdb_transaction OBJECT undo_buffer.cpp transaction_context.cpp transaction.cpp transaction_manager.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_transaction> PARENT_SCOPE)
