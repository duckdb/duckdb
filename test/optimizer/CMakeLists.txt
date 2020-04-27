add_library_unity(test_optimizer
                  OBJECT
                  test_arithmetic_simplification.cpp
                  test_case_simplification.cpp
                  test_comparison_simplification.cpp
                  test_conjunction_simplification.cpp
                  test_constant_folding.cpp
                  test_cse_optimizer.cpp
                  test_distributivity_rule.cpp
                  test_move_constants.cpp
                  test_index_scan_optimizer.cpp
                  test_filter_pushdown_optimizer.cpp
                  test_topn_optimizer.cpp
                  test_like_optimizer.cpp
                  test_needle_optimizer.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_optimizer>
    PARENT_SCOPE)
