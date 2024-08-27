add_library_unity(
  duckdb_func_date
  OBJECT
  age.cpp
  current.cpp
  epoch.cpp
  date_diff.cpp
  date_part.cpp
  date_sub.cpp
  date_trunc.cpp
  make_date.cpp
  strftime.cpp
  time_bucket.cpp
  to_interval.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_date>
    PARENT_SCOPE)
