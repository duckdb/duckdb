add_library_unity(duckdb_main_secret OBJECT secret.cpp duck_secret_manager.cpp
                  debug_secret_manager.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_secret>
    PARENT_SCOPE)
