
add_subdirectory(operator)
add_subdirectory(vector)

add_library(duckdb_execution OBJECT executor.cpp expression_executor.cpp physical_operator.cpp physicalplangenerator.cpp datachunk.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_execution> PARENT_SCOPE)
