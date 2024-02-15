add_library_unity(
  duckdb_common_exception OBJECT binder_exception.cpp catalog_exception.cpp
  conversion_exception.cpp parser_exception.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_exception>
    PARENT_SCOPE)
