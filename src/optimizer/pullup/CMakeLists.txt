add_library_unity(duckdb_optimizer_pullup
                  OBJECT
                  pullup_filter.cpp
                  pullup_projection.cpp
                  pullup_left_join.cpp
                  pullup_both_side.cpp
                  pullup_intersect.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_optimizer_pullup> PARENT_SCOPE)
