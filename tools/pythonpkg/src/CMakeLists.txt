# this is used for clang-tidy checks
find_package(PythonLibs REQUIRED)
find_package(pybind11 REQUIRED)
include_directories(${pybind11_INCLUDE_DIR})

add_subdirectory(pyrelation)
add_subdirectory(common)
add_subdirectory(pandas)

add_library(
  python_src OBJECT
  array_wrapper.cpp
  pyconnection.cpp
  python_import_cache.cpp
  pyrelation.cpp
  pyresult.cpp
  map.cpp
  vector_conversion.cpp
  python_conversion.cpp
  python_objects.cpp
  arrow_array_stream.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:python_src>
    PARENT_SCOPE)
