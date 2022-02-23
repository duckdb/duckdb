add_library_unity(duckdb_main_settings OBJECT settings.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_settings>
    PARENT_SCOPE)
