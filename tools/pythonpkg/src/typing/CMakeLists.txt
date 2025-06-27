# this is used for clang-tidy checks
add_library(python_type OBJECT pytype.cpp typing.cpp)

target_link_libraries(python_type PRIVATE pytooling)
