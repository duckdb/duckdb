add_subdirectory(system)
add_subdirectory(version)
add_subdirectory(arrow)
add_library_unity(
  duckdb_func_table
  OBJECT
  arrow.cpp
  arrow_conversion.cpp
  checkpoint.cpp
  glob.cpp
  query_function.cpp
  range.cpp
  repeat.cpp
  repeat_row.cpp
  copy_csv.cpp
  read_csv.cpp
  sniff_csv.cpp
  read_file.cpp
  system_functions.cpp
  summary.cpp
  table_scan.cpp
  unnest.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_table>
    PARENT_SCOPE)
