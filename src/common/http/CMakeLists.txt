include_directories(../../../third_party/httplib/)

add_library_unity(duckdb_common_http OBJECT http_util.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_http>
    PARENT_SCOPE)
