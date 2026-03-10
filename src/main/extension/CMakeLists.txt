add_library(
  duckdb_main_extension OBJECT
  extension_alias.cpp extension_helper.cpp extension_install.cpp
  extension_load.cpp extension_loader.cpp)

# Escape backslashes in the string so the compiler sees a valid C string literal
string(REPLACE "\\" "\\\\" EXT_DIR_ESCAPED "${EXTENSION_DIRECTORIES}")
# Escape ; in the string to ensure this is treated as a plain string instead of
# a list
string(REPLACE ";" "\;" EXT_DIR_ESCAPED "${EXT_DIR_ESCAPED}")

target_compile_definitions(
  duckdb_main_extension
  PRIVATE "DUCKDB_EXTENSION_DIRECTORIES=\"${EXT_DIR_ESCAPED}\"")

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_extension>
    PARENT_SCOPE)
