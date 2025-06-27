# this is used for clang-tidy checks
add_library(python_native OBJECT python_objects.cpp python_conversion.cpp)

target_link_libraries(python_native PRIVATE pytooling)
