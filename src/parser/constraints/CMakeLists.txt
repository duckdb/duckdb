add_library_unity(duckdb_constraints
                  OBJECT
                  check_constraint.cpp
                  not_null_constraint.cpp
                  unique_constraint.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_constraints>
    PARENT_SCOPE)
