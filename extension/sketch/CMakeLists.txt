cmake_minimum_required(VERSION 2.8.12)

project(SketchExtension)

include_directories(include)
include_directories(third_party/apache-datasketches/common/include)
include_directories(third_party/apache-datasketches/hll/include)

# add_subdirectory(third_party)

set(SKETCH_EXTENSION_FILES sketch-extension.cpp sketch-hll.cpp)

add_library(sketch_extension STATIC ${SKETCH_EXTENSION_FILES})
link_threads(sketch_extension)
disable_target_warnings(sketch_extension)

build_loadable_extension(sketch ${SKETCH_EXTENSION_FILES})
install(
  TARGETS sketch_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
