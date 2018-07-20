
add_library(duckdb_execution OBJECT physicaloperator.cpp seqscan.cpp physicalplangenerator.cpp datachunk.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_execution> PARENT_SCOPE)
