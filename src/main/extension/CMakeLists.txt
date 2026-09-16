# One weak no-op duckdb_extension_<name>_root per known extension, each its own
# object so the engine archive supplies exactly the names whose extension
# archive is absent from a link
set(AUTOLINK_FALLBACK_FILES)
foreach(NAME IN LISTS DUCKDB_KNOWN_EXTENSIONS)
  set(FALLBACK_FILE
      "${PROJECT_BINARY_DIR}/codegen/autolink/${NAME}_root_fallback.cpp")
  configure_file(${PROJECT_SOURCE_DIR}/extension/loader/root_fallback.cpp.in
                 ${FALLBACK_FILE} @ONLY)
  list(APPEND AUTOLINK_FALLBACK_FILES ${FALLBACK_FILE})
endforeach()
add_library(duckdb_autolink_fallbacks OBJECT ${AUTOLINK_FALLBACK_FILES})
# the same objects as an archive, taken after the extension archives when a link
# wants them that way
add_library(duckdb_autolink_fallbacks_archive STATIC
            $<TARGET_OBJECTS:duckdb_autolink_fallbacks>)

add_library(
  duckdb_main_extension OBJECT
  extension_alias.cpp
  extension_helper.cpp
  extension_install.cpp
  extension_load.cpp
  extension_loader.cpp
  extension_repository_manager.cpp
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
