add_library_unity(duckdb_planner_filter OBJECT conjunction_filter.cpp
                  constant_filter.cpp null_filter.cpp struct_filter.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_planner_filter>
    PARENT_SCOPE)
