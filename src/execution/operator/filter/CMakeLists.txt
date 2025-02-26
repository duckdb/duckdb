add_library_unity(duckdb_operator_filter OBJECT physical_filter.cpp physical_use_bf.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_filter>
    PARENT_SCOPE)
