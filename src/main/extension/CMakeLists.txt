# Conditionally compile installing and loading external extensions based on
# whether extension loading is enabled or not
if(ENABLE_EXTENSION_LOAD)
  set(EXTENSION_LOAD_SOURCES extension_install_dynamic.cpp
                             extension_load_dynamic.cpp)
else()
  set(EXTENSION_LOAD_SOURCES extension_load_none.cpp)
endif()

add_library(
  duckdb_main_extension OBJECT
  extension_alias.cpp
  extension_helper.cpp
  extension_install.cpp
  extension_load.cpp
  extension_loader.cpp
  extension_repository_manager.cpp
  linked_extension_registry.cpp
  ${EXTENSION_LOAD_SOURCES})

# Escape ; in the string to ensure this is treated as a plain string instead of
# a list
string(REPLACE ";" "\;" EXT_DIR_ESCAPED "${EXTENSION_DIRECTORIES}")

target_compile_definitions(
  duckdb_main_extension
  PRIVATE "DUCKDB_EXTENSION_DIRECTORIES=\"${EXT_DIR_ESCAPED}\"")

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_extension>
    PARENT_SCOPE)
