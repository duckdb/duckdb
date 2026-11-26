add_library_unity(
  duckdb_optimizer_relation_statistics
  OBJECT
  equality_cardinality.cpp
  equality_evidence.cpp
  equality_projection.cpp
  finite_filter_selection.cpp
  relation_statistics.cpp
  relation_statistics_extractor.cpp
  relation_statistics_helper.cpp
  relation_statistics_operator.cpp
  relation_statistics_scan.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_optimizer_relation_statistics>
    PARENT_SCOPE)
