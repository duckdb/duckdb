add_library_unity(duckdb_optimizer_matcher OBJECT expression_matcher.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_optimizer_matcher> PARENT_SCOPE)
