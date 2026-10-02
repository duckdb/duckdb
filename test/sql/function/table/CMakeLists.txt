add_library_unity(
  test_table_function OBJECT table_in_out.cpp table_bind_replace.cpp
  table_partition_info.cpp table_named_parameter_metadata.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_table_function>
    PARENT_SCOPE)
