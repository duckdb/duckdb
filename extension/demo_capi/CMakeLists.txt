cmake_minimum_required(VERSION 2.8.12...3.29)

project(DemoCapiExtension)

build_loadable_extension(demo_capi "" capi_demo.cpp)
