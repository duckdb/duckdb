cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME kafkaredo)

option(OSX_BUILD_UNIVERSAL "Build both architectures on OSX and create a single binary containing both." FALSE)
if (OSX_BUILD_UNIVERSAL)
    if (NOT APPLE)
        error("This only makes sense on OSX")
    endif()
    SET(CMAKE_OSX_ARCHITECTURES "x86_64;arm64" CACHE STRING "Build architectures for Mac OS X" FORCE)
    set(CMAKE_OSX_DEPLOYMENT_TARGET 11.0 CACHE STRING "Minimum OS X deployment version" FORCE)
endif()

project(${TARGET_NAME})

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

find_package(RdKafka CONFIG REQUIRED)
include_directories(include)

set(EXTENSION_SOURCES kafkaredo_extension.cpp kafkafs.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} RdKafka::rdkafka RdKafka::rdkafka++)
target_link_libraries(${LOADABLE_EXTENSION_NAME} RdKafka::rdkafka RdKafka::rdkafka++)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
