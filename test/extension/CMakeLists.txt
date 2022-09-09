 set(INGORE_WARNINGS OFF)
 build_loadable_extension_directory(
 loadable_extension_demo test/extension loadable_extension_demo.cpp
 ${IGNORE_WARNINGS})

build_loadable_extension_directory(
  replacement_open_extension_demo test/extension
  replacement_open_extension_demo.cpp ${IGNORE_WARNINGS})
