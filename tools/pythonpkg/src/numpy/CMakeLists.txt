# this is used for clang-tidy checks
include_directories(${pybind11_INCLUDE_DIR})
include_directories(${PYTHON_INCLUDE_DIRS})
find_package(pybind11 REQUIRED)

add_library(
  python_numpy OBJECT
  type.cpp numpy_scan.cpp array_wrapper.cpp raw_array_wrapper.cpp
  numpy_bind.cpp numpy_result_conversion.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:python_numpy>
    PARENT_SCOPE)
