add_library_unity(
  duckdb_core_functions_generic
  OBJECT
  alias.cpp
  binning.cpp
  cast_to_type.cpp
  can_implicitly_cast.cpp
  current_setting.cpp
  hash.cpp
  least.cpp
  replace_type.cpp
  stats.cpp
  typeof.cpp
  system_functions.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_generic>
    PARENT_SCOPE)
