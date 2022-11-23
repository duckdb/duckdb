find_package(PythonLibs)
find_package(pybind11)
if(NOT pybind11_FOUND)
  return()
endif()
if(NOT PythonLibs_FOUND)
  return()
endif()
include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/../../)
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${pybind11_INCLUDE_DIR})
# this is used for clang-tidy checks

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
