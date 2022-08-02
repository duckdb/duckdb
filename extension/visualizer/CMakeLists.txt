cmake_minimum_required(VERSION 2.8.12)

project(VisualizerExtension)

include_directories(include)

add_library(visualizer_extension STATIC visualizer-extension.cpp)
set(INGORE_WARNINGS OFF)
build_loadable_extension(visualizer visualizer-extension.cpp ${IGNORE_WARNINGS})
install(
  TARGETS visualizer_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
