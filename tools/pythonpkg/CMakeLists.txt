include_directories(src)

find_package(PythonLibs)
find_package(pybind11)
if(NOT pybind11_FOUND)
  return()
endif()
if(NOT PythonLibs_FOUND)
  return()
endif()
include_directories(../../)
include_directories(src/include)
include_directories(${pybind11_INCLUDE_DIR})
# this is used for clang-tidy checks
add_library(
  python_duckdb
  duckdb_python.cpp
  src/array_wrapper.cpp
  src/pandas_scan.cpp
  src/pyconnection.cpp
  src/pyrelation.cpp
  src/pyresult.cpp
  src/map.cpp
  src/exceptions.cpp
  src/vector_conversion.cpp
  src/arrow_array_stream.cpp)
target_include_directories(python_duckdb PRIVATE ${PYTHON_INCLUDE_DIRS})
target_link_libraries(python_duckdb duckdb_static ${PYTHON_LIBRARIES})
