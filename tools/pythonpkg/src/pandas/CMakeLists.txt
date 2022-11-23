# this is used for clang-tidy checks
find_package(PythonLibs REQUIRED)
find_package(pybind11 REQUIRED)
include_directories(${pybind11_INCLUDE_DIR})

add_library(python_pandas OBJECT type.cpp scan.cpp analyzer.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:python_pandas>
    PARENT_SCOPE)
