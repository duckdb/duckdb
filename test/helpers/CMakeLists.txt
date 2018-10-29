
add_library(test_helpers STATIC expression_helper.cpp capi_helpers.cpp test_helpers.cpp)
target_link_libraries(test_helpers duckdb)
