# this is used for clang-tidy checks
add_library(python_common OBJECT exceptions.cpp)

target_link_libraries(python_common PRIVATE pytooling)
