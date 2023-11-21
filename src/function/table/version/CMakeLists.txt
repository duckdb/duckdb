add_definitions(-DDUCKDB_SOURCE_ID="\""${GIT_COMMIT_HASH}"\"")
add_definitions(-DDUCKDB_VERSION="\""${DUCKDB_VERSION}"\"")

add_library_unity(duckdb_func_table_version OBJECT pragma_version.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_table_version>
    PARENT_SCOPE)
