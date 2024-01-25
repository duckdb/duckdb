add_library_unity(
        duckdb_common_exception
        OBJECT
        catalog_exception.cpp
        parser_exception.cpp)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_exception>
        PARENT_SCOPE)
