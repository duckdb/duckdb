add_library_unity(
  duckdb_func_generic
  OBJECT
  alias.cpp
  current_setting.cpp
  error.cpp
  hash.cpp
  least.cpp
  stats.cpp
  typeof.cpp
  system_functions.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_generic>
    PARENT_SCOPE)
