# this is used for clang-tidy checks
add_library(python_expression OBJECT initialize.cpp)

target_link_libraries(python_expression PRIVATE pytooling)
