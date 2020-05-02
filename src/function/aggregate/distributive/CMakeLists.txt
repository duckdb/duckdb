add_library_unity(duckdb_aggr_distr
                  OBJECT
                  bitagg.cpp
                  count.cpp
                  first.cpp
                  minmax.cpp
                  string_agg.cpp
                  sum.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_aggr_distr>
    PARENT_SCOPE)
