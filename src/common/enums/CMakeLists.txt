add_library_unity(
  duckdb_common_enums
  OBJECT
  catalog_type.cpp
  compression_type.cpp
  date_part_specifier.cpp
  expression_type.cpp
  file_compression_type.cpp
  join_type.cpp
  logical_operator_type.cpp
  metric_type.cpp
  optimizer_type.cpp
  physical_operator_type.cpp
  statement_type.cpp
  relation_type.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_enums>
    PARENT_SCOPE)
