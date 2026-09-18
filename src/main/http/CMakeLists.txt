include_directories(../../../third_party/httplib)

add_library_unity(
  duckdb_common_http
  OBJECT
  http_client_pool.cpp
  http_retry_budget.cpp
  http_transport_config.cpp
  http_transport_manager.cpp
  http_util.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_http>
    PARENT_SCOPE)
