add_library_unity(
  test_optimizer
  OBJECT
  aggregate_rewrite.cpp
  build_probe_side_optimizer.cpp
  column_binding_rewrite.cpp
  common_subplan_nonserializable.cpp
  filter_pushdown_alias.cpp
  join_order_conflict_detector.cpp
  relation_statistics.cpp
  union_alls.cpp)

set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_optimizer>
    PARENT_SCOPE)
