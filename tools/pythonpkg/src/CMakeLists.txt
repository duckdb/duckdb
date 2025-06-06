# this is used for clang-tidy checks
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
  pystatement.cpp
  python_import_cache.cpp
  python_replacement_scan.cpp
  python_dependency.cpp
  pyrelation.cpp
  pyexpression.cpp
  dataframe.cpp
  pyresult.cpp
  pyfilesystem.cpp
  map.cpp)

target_link_libraries(python_src PRIVATE pytooling)
