add_library_unity(
  test_sql_date
  OBJECT
  test_date.cpp
  test_date_part.cpp
  test_date_trunc.cpp
  test_extract.cpp
  test_time.cpp
  test_timestamp.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_date>
    PARENT_SCOPE)
