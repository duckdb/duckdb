# this is used for clang-tidy checks
add_library(python_relation OBJECT initialize.cpp)

target_link_libraries(python_relation PRIVATE pytooling)
