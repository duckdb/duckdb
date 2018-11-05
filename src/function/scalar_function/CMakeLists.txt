
add_library(duckdb_scalar_function OBJECT abs.cpp date_part.cpp substring.cpp year.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_scalar_function> PARENT_SCOPE)
