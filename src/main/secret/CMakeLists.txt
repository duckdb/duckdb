add_library_unity(duckdb_main_secret OBJECT secret.cpp secret_manager.cpp
                  secret_storage.cpp default_secrets.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_secret>
    PARENT_SCOPE)
