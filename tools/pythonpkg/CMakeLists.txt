# ────────────────────────────────────────────
# IMPORTANT: Cmake is NOT used to build this library. This is here only for
# Intellisense and clang-tidy.
#
# See README.md for more information on how to use this file and how to build
# the library.
# ────────────────────────────────────────────

cmake_minimum_required(VERSION 3.18...3.29)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ────────────────────────────────────────────
# Policy hygiene
# ────────────────────────────────────────────
if(POLICY CMP0148) # Disallow FindPythonLibs
  cmake_policy(SET CMP0148 NEW)
endif()

if(POLICY CMP0003) # No implicit link directories
  cmake_policy(SET CMP0003 NEW)
endif()

# ────────────────────────────────────────────
# Dependencies
# ────────────────────────────────────────────
find_package(Python3 REQUIRED COMPONENTS Interpreter Development)
message(STATUS "CPython executable: ${Python3_EXECUTABLE}")
message(STATUS "CPython include dirs: ${Python3_INCLUDE_DIRS}")
message(STATUS "CPython libraries: ${Python3_LIBRARIES}")

find_package(pybind11 CONFIG QUIET)
# Header-only shim: good enough for clang-tidy / IntelliSense
if(NOT TARGET pybind11::pybind11)
  add_library(pybind11::pybind11 INTERFACE IMPORTED)
  target_include_directories(
    pybind11::pybind11
    INTERFACE $<BUILD_INTERFACE:${Python3_SITEARCH}/pybind11/include>)
endif()

# ────────────────────────────────────────────
# INTERFACE library that **only** carries usage requirements
# ────────────────────────────────────────────
add_library(pytooling INTERFACE)

# 1. Include paths that clang‑tidy / IntelliSense must see
target_include_directories(
  pytooling INTERFACE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/include>
                      $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src/include>)

# 1. All the imported dependency targets: they bring their own include dirs +
#   compile definitions automatically.
target_link_libraries(
  pytooling INTERFACE Python3::Python # defined by FindPython3
                      pybind11::pybind11 # defined by pybind11Config.cmake
)

# ────────────────────────────────────────────
# Descend into the real DuckDB‑Python sources
# ────────────────────────────────────────────
add_subdirectory(src EXCLUDE_FROM_ALL)

# ────────────────────────────────────────────
# Fake aggregation library for analysis tools
# ────────────────────────────────────────────
add_library(duckdb_python_analysis STATIC duckdb_python.cpp)
target_link_libraries(
  duckdb_python_analysis
  PRIVATE pytooling # brings Python + pybind11 + local includes
          python_src # keep in sync with sub-directory object libs
          python_arrow
          python_common
          python_functional
          python_jupyter
          python_native
          python_numpy
          python_pandas
          python_pybind11
          python_connection
          python_expression
          python_relation
          python_type
          duckdb_static # <- DuckDB static library from the main project
)
