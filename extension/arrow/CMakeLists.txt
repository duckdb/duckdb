cmake_minimum_required(VERSION 2.8.12)

project(ArrowExtension)

#add_extension_definitions()

include_directories(include ../..)
set(ARROW_SOURCES
        arrow-extension.cpp
        arrow_stream_buffer.cpp)

add_library(arrow_extension STATIC ${ARROW_SOURCES})
set(PARAMETERS "-warnings")
build_loadable_extension(arrow ${PARAMETERS} ${ARROW_SOURCES})

# For dynamic link
FIND_PACKAGE(Arrow REQUIRED)
target_link_libraries(arrow_loadable_extension arrow_shared)
target_link_libraries(arrow_extension arrow_shared)

# For static link TODO WIP
#include_directories(/usr/local/include/)
#set_property(TARGET arrow_loadable_extension PROPERTY CXX_STANDARD 17)
#set_property(TARGET arrow_extension PROPERTY CXX_STANDARD 17)
#target_link_libraries(arrow_loadable_extension /usr/local/lib/libarrow.a)
#target_link_libraries(arrow_extension /usr/local/lib/libarrow.a)

install(
  TARGETS arrow_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
