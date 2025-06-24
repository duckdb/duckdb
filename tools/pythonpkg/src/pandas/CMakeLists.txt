# this is used for clang-tidy checks
add_library(python_pandas OBJECT scan.cpp analyzer.cpp bind.cpp)

target_link_libraries(python_pandas PRIVATE pytooling)
