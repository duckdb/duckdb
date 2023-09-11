# this is used for clang-tidy checks

find_package(pybind11 REQUIRED)

include_directories(${pybind11_INCLUDE_DIR})
include_directories(${PYTHON_INCLUDE_DIRS})

add_subdirectory(pyrelation)
add_subdirectory(pyexpression)
add_subdirectory(pybind11)
add_subdirectory(numpy)
add_subdirectory(native)
add_subdirectory(jupyter)
add_subdirectory(typing)
add_subdirectory(functional)
add_subdirectory(pyconnection)
add_subdirectory(common)
add_subdirectory(pandas)
add_subdirectory(arrow)

add_library(
  python_src OBJECT
  path_like.cpp
  python_udf.cpp
  pyconnection.cpp
  python_import_cache.cpp
  pyrelation.cpp
  pyexpression.cpp
  dataframe.cpp
  pyresult.cpp
  pyfilesystem.cpp
  map.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:python_src>
    PARENT_SCOPE)
