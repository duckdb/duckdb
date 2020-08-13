add_library_unity(test_optimizer
                  OBJECT
                  test_cse_optimizer.cpp
                  test_index_scan_optimizer.cpp
                  test_filter_pushdown_optimizer.cpp
                  test_topn_optimizer.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_optimizer>
    PARENT_SCOPE)
