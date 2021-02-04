include_directories(src)

find_package(PythonLibs REQUIRED)
find_package(pybind11 REQUIRED)
include_directories(../../)

add_library(python_duckdb duckdb_python.cpp)
target_include_directories(python_duckdb PRIVATE ${PYTHON_INCLUDE_DIRS})
target_link_libraries(python_duckdb duckdb_static ${PYTHON_LIBRARIES})
