add_library_unity(
  duckdb_parsed_data
  OBJECT
  alter_function_info.cpp
  alter_info.cpp
  alter_table_info.cpp
  create_info.cpp
  create_index_info.cpp
  create_scalar_function_info.cpp
  create_table_info.cpp
  sample_options.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_parsed_data>
    PARENT_SCOPE)
