add_library(
  duckdb_main_extension OBJECT
  extension_alias.cpp
  extension_helper.cpp
  extension_install.cpp
  extension_load.cpp
  extension_loader.cpp
  extension_repository_manager.cpp
  external_extension_provider.cpp
  linked_extension_registry.cpp)

# Escape ; in the string to ensure this is treated as a plain string instead of
# a list
string(REPLACE ";" "\;" EXT_DIR_ESCAPED "${EXTENSION_DIRECTORIES}")

target_compile_definitions(
  duckdb_main_extension
  PRIVATE "DUCKDB_EXTENSION_DIRECTORIES=\"${EXT_DIR_ESCAPED}\"")

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_extension>
    PARENT_SCOPE)

# Installing and loading external extensions is its own archive, registered like
# a static extension through duckdb_extension_loadable_extensions_describe
if(ENABLE_EXTENSION_LOAD)
  add_library(duckdb_loadable_extensions STATIC extension_install_dynamic.cpp
                                                extension_load_dynamic.cpp)
  target_link_libraries(duckdb_loadable_extensions duckdb_static)
  install(
    TARGETS duckdb_loadable_extensions
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
endif()

# The local_extension_repository capability: automatic installs default to this
# build's extension repository
file(TO_CMAKE_PATH "${LOCAL_EXTENSION_REPO_DIR}" LOCAL_EXTENSION_REPOSITORY)
configure_file(local_extension_repository.cpp.in
               ${CMAKE_CURRENT_BINARY_DIR}/local_extension_repository.cpp @ONLY)
add_library(duckdb_local_extension_repository STATIC
            ${CMAKE_CURRENT_BINARY_DIR}/local_extension_repository.cpp)
target_link_libraries(duckdb_local_extension_repository duckdb_static)
