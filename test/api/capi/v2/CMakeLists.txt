add_library_unity(
  test_sql_capi_v2
  OBJECT
  test_capi_v2.cpp
  test_capi_v2_basic.cpp
  test_capi_v2_arena.cpp
  test_capi_v2_arrow.cpp
  test_capi_v2_bytes.cpp
  test_capi_v2_catalog.cpp
  test_capi_v2_cast_function.cpp
  test_capi_v2_column_data_collection.cpp
  test_capi_v2_copy_function.cpp
  test_capi_v2_custom_type.cpp
  test_capi_v2_data_chunk.cpp
  test_capi_v2_error_info.cpp
  test_capi_v2_expression.cpp
  test_capi_v2_file_system.cpp
  test_capi_v2_identifier.cpp
  test_capi_v2_logical_type.cpp
  test_capi_v2_option.cpp
  test_capi_v2_prepared_statement.cpp
  test_capi_v2_qname.cpp
  test_capi_v2_query.cpp
  test_capi_v2_replacement_scan.cpp
  test_capi_v2_result.cpp
  test_capi_v2_aggregate_function.cpp
  test_capi_v2_scalar_function.cpp
  test_capi_v2_statement.cpp
  test_capi_v2_table_function.cpp
  test_capi_v2_static_extension.cpp
  test_capi_v2_value.cpp
  test_capi_v2_vector_write.cpp)

# The extension itself is built on its own: the entrypoint macro claims fixed
# names, so it cannot share a unity TU.
add_library(test_capi_v2_static_extension OBJECT
            static_extension/cpp_api_static_demo.cpp)
target_compile_definitions(test_capi_v2_static_extension
                           PRIVATE DUCKDB_EXTENSION_NAME=cpp_api_static_demo)
target_include_directories(test_capi_v2_static_extension
                           PRIVATE ${CMAKE_SOURCE_DIR}/tools/cpp)

set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_capi_v2>
    $<TARGET_OBJECTS:test_capi_v2_static_extension>
    PARENT_SCOPE)
