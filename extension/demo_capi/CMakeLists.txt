cmake_minimum_required(VERSION 2.8.12...3.29)

project(DemoCapiExtension)

include_directories(include)

set(CAPI_VERSION "v0.0.1")

add_definitions(-DCAPI_VERSION=${CAPI_VERSION})

build_loadable_extension_capi(demo_capi ${CAPI_VERSION} capi_demo.cpp
                              add_numbers.cpp)
