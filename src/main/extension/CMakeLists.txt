add_library(
  duckdb_main_extension OBJECT
  extension_alias.cpp extension_helper.cpp extension_install.cpp
  extension_load.cpp extension_loader.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_extension>
    PARENT_SCOPE)

add_library(dummy_static_extension_loader dummy_static_extension_loader.cpp)
