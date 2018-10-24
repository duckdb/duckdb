
add_subdirectory(scalar_function)
add_subdirectory(table_function)

add_library(duckdb_function OBJECT function.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_function> PARENT_SCOPE)
