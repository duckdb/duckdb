include_directories(../../third_party/libpg_query/include)
include_directories(../../third_party/libpg_query)

add_subdirectory(constraints)
add_subdirectory(expression)
add_subdirectory(parsed_data)
add_subdirectory(query_node)
add_subdirectory(statement)
add_subdirectory(tableref)
add_subdirectory(transform)

add_library_unity(
  duckdb_parser
  OBJECT
  base_expression.cpp
  column_definition.cpp
  column_list.cpp
  constraint.cpp
  expression_util.cpp
  keyword_helper.cpp
  parsed_expression.cpp
  parsed_expression_iterator.cpp
  parser.cpp
  query_error_context.cpp
  query_node.cpp
  result_modifier.cpp
  tableref.cpp
  transformer.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_parser>
    PARENT_SCOPE)
