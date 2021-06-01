cmake_minimum_required(VERSION 2.8.12)

project(VisualizerExtension)

include_directories(include)

add_library(visualizer_extension STATIC visualizer-extension.cpp)
target_compile_definitions(
  visualizer_extension PUBLIC VIS_RESOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/")
