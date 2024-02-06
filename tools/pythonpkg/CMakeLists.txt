include_directories(src)
include_directories(../../)
include_directories(src/include)
include_directories(${pybind11_INCLUDE_DIR})
include_directories(${PYTHON_INCLUDE_DIRS})

add_subdirectory(src)

find_package(PythonLibs)
if(NOT PythonLibs_FOUND)
  return()
endif()

find_package(pybind11)
if(NOT pybind11_FOUND)
  return()
endif()
# this is used for clang-tidy checks

set(ALL_OBJECT_FILES duckdb_python.cpp ${ALL_OBJECT_FILES})

add_library(python_duckdb ${ALL_OBJECT_FILES})

# Get Python include directories
execute_process(
  COMMAND python3-config --includes
  OUTPUT_VARIABLE PYTHON_INCLUDE_DIRS
  OUTPUT_STRIP_TRAILING_WHITESPACE)

# Get Python libraries and flags
execute_process(
  COMMAND python3-config --libs
  OUTPUT_VARIABLE PYTHON_LIBRARIES
  OUTPUT_STRIP_TRAILING_WHITESPACE)

target_include_directories(python_duckdb PRIVATE ${PYTHON_INCLUDE_DIRS})
target_link_libraries(python_duckdb duckdb_static ${PYTHON_LIBRARIES})
