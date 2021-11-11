include_directories(../../../third_party/httplib/)

add_library_unity(duckdb_main_extension OBJECT extension_helper.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_extension>
    PARENT_SCOPE)

add_extension_definitions(duckdb_main_extension)
