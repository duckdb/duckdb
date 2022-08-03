add_library_unity(duckdb_common_arrow OBJECT arrow_appender.cpp
                  arrow_converter.cpp arrow_wrapper.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_arrow>
    PARENT_SCOPE)
