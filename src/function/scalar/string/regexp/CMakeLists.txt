add_library_unity(duckdb_func_string_regexp OBJECT regexp_util.cpp
                  regexp_extract_all.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_string_regexp>
    PARENT_SCOPE)
