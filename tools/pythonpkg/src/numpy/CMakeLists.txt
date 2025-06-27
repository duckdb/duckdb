# this is used for clang-tidy checks
add_library(
  python_numpy OBJECT
  type.cpp numpy_scan.cpp array_wrapper.cpp raw_array_wrapper.cpp
  numpy_bind.cpp numpy_result_conversion.cpp)

target_link_libraries(python_numpy PRIVATE pytooling)
