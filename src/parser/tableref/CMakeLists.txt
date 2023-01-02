add_library_unity(
  duckdb_parser_tableref
  OBJECT
  basetableref.cpp
  crossproductref.cpp
  emptytableref.cpp
  expressionlistref.cpp
  joinref.cpp
  pos_join_ref.cpp
  subqueryref.cpp
  table_function.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_parser_tableref>
    PARENT_SCOPE)
