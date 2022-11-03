add_library(
  python_src OBJECT
  array_wrapper.cpp
  pandas_scan.cpp
  pyconnection.cpp
  python_import_cache.cpp
  pyrelation.cpp
  pyresult.cpp
  map.cpp
  exceptions.cpp
  vector_conversion.cpp
  python_conversion.cpp
  python_objects.cpp
  pandas_type.cpp
  pandas_analyzer.cpp
  arrow_array_stream.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:python_src>
    PARENT_SCOPE)
