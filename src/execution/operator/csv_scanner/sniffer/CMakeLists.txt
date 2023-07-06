add_library_unity(
  duckdb_operator_csv_sniffer
  OBJECT
  csv_sniffer.cpp
  dialect_detection.cpp
  header_detection.cpp
  type_detection.cpp
  type_refinement.cpp
  type_replacement.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_csv_sniffer>
    PARENT_SCOPE)
