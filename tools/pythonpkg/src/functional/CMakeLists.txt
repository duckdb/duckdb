# this is used for clang-tidy checks
add_library(python_functional OBJECT functional.cpp)

target_link_libraries(python_functional PRIVATE pytooling)
