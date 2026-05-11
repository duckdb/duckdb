add_library_unity(
  duckdb_parser_peg_transformer_generated OBJECT transform_detach_generated.cpp
  transform_export_generated.cpp transform_transaction_generated.cpp
  transform_use_generated.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    $<TARGET_OBJECTS:duckdb_parser_peg_transformer_generated>
    PARENT_SCOPE)
