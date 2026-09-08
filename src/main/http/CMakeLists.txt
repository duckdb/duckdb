include_directories(../../../third_party/httplib)

# work-around for httplib
if(${EXIT_TIME_DESTRUCTORS_WARNING})
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -Wno-exit-time-destructors")
endif()

add_library_unity(duckdb_common_http OBJECT http_client_pool.cpp
                  http_transport_manager.cpp http_util.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_http>
    PARENT_SCOPE)
