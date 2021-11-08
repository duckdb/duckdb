cmake_minimum_required(VERSION 2.8.12)

project(VisualizerExtension)

include_directories(include)

add_library(visualizer_extension STATIC visualizer-extension.cpp)

build_loadable_extension(visualizer visualizer-extension.cpp)
