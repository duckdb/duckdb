add_library_unity(
  duckdb_core_functions_date
  OBJECT
  current.cpp
  age.cpp
  date_diff.cpp
  date_sub.cpp
  to_interval.cpp
  time_bucket.cpp
  date_trunc.cpp
  epoch.cpp
  date_part.cpp
  make_date.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_date>
    PARENT_SCOPE)
