add_definitions(-DDUCKDB_SOURCE_ID="\""${GIT_COMMIT_HASH}"\"")

add_library_unity(duckdb_func_sqlite OBJECT pragma_collations.cpp
                  pragma_table_info.cpp pragma_version.cpp sqlite_master.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_sqlite>
    PARENT_SCOPE)
