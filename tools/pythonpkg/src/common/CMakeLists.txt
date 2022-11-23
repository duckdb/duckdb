# this is used for clang-tidy checks
find_package(PythonLibs REQUIRED)
find_package(pybind11 REQUIRED)
include_directories(${pybind11_INCLUDE_DIR})

add_library(python_common OBJECT exceptions.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:python_common>
    PARENT_SCOPE)
