# Conditionally compile the default HTTP client based on whether the built-in
# httplib is enabled or not
if(ENABLE_BUILTIN_HTTPLIB)
  include_directories(../../../third_party/httplib)
  set(HTTP_CLIENT_SOURCE http_client_httplib.cpp)
else()
  set(HTTP_CLIENT_SOURCE http_client_none.cpp)
endif()

add_library_unity(
  duckdb_common_http
  OBJECT
  http_client_pool.cpp
  http_retry_budget.cpp
  http_transport_config.cpp
  http_transport_manager.cpp
  http_util.cpp
  ${HTTP_CLIENT_SOURCE})
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_http>
    PARENT_SCOPE)
