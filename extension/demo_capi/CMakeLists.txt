cmake_minimum_required(VERSION 2.8.12...3.29)

project(DemoCapiExtension)

include_directories(include)

set(EXTENSION_NAME demo_capi)
set(CAPI_VERSION v0.0.1)

add_definitions(-DDUCKDB_EXTENSION_CAPI_VERSION=${CAPI_VERSION})
add_definitions(-DDUCKDB_EXTENSION_NAME=${EXTENSION_NAME})

build_loadable_extension_capi(${EXTENSION_NAME} ${CAPI_VERSION} capi_demo.cpp
                              add_numbers.cpp)
