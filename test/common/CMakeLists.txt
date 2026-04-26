add_library_unity(
  test_common
  OBJECT
  caching_test_utils.cpp
  test_cast.cpp
  test_checksum.cpp
  test_column_data_allocator.cpp
  test_file_buffer_handle_group.cpp
  test_file_system.cpp
  test_caching_file_system_wrapper.cpp
  test_external_file_cache.cpp
  test_hyperlog.cpp
  test_lru_cache.cpp
  test_numeric_cast.cpp
  test_parse_expression.cpp
  test_parse_logical_type.cpp
  test_utf.cpp
  test_allocator_debug_info.cpp
  test_buffer_pool_reservation.cpp
  test_storage_fuzz.cpp
  test_strftime.cpp
  test_string_stats.cpp
  test_string_util.cpp)

set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_common>
    PARENT_SCOPE)
