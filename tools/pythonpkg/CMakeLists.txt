include_directories(src)

find_package(PythonLibs REQUIRED)

include_directories(.eggs/pybind11-2.6.1-py3.6.egg/pybind11/include/)
include_directories(../../)

add_library(python_duckdb duckdb_python.cpp)
target_include_directories(python_duckdb PRIVATE ${PYTHON_INCLUDE_DIRS})
target_link_libraries(python_duckdb duckdb_static ${PYTHON_LIBRARIES})
