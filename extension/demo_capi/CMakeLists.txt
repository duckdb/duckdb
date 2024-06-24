cmake_minimum_required(VERSION 2.8.12...3.29)

project(DemoCapiExtension)

include_directories(include)

build_loadable_extension(demo_capi "" capi_demo.cpp add_numbers.cpp)
