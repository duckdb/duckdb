add_library_unity(duckdb_aggregate_function
                  OBJECT
                  count_star.cpp
                  count.cpp
                  first.cpp
                  max.cpp
                  min.cpp
                  sum.cpp
                  covar.cpp
                  stddev_samp.cpp
                  distributive.cpp
                  avg.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_aggregate_function> PARENT_SCOPE)
