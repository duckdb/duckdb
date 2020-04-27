add_library_unity(duckdb_optimizer_rules
                  OBJECT
                  arithmetic_simplification.cpp
                  case_simplification.cpp
                  comparison_simplification.cpp
                  conjunction_simplification.cpp
                  constant_folding.cpp
                  date_part_simplification.cpp
                  distributivity.cpp
                  empty_needle_removal.cpp
                  move_constants.cpp
                  like_optimizations.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_optimizer_rules> PARENT_SCOPE)
