cmake_minimum_required(VERSION 3.5...3.29)

project(WithinIndexExtension)

include_directories(include)

set(EXTENSION_NAME within_index)
set(EXTENSION_FILES within_index_extension.cpp)

build_static_extension(${EXTENSION_NAME} ${EXTENSION_FILES})
