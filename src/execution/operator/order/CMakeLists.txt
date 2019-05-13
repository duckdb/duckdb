add_library_unity(duckdb_operator_order OBJECT physical_order.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_order> PARENT_SCOPE)
