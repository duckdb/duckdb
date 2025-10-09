add_library_unity(
  duckdb_peg_transformer
  OBJECT
  peg_transformer.cpp
  peg_transformer_factory.cpp
  transform_attach.cpp
  transform_common.cpp
  transform_create_table.cpp
  transform_detach.cpp
  transform_expression.cpp
  transform_load.cpp
  transform_set.cpp
  transform_use.cpp)
set(AUTOCOMPLETE_EXTENSION_FILES
    ${AUTOCOMPLETE_EXTENSION_FILES} $<TARGET_OBJECTS:duckdb_peg_transformer>
    PARENT_SCOPE)
