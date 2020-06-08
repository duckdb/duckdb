cmake_minimum_required(VERSION 2.8.7)

project(ICUExtension)

include_directories(include)

add_library(icu_extension_static STATIC icu-collate.cpp icu-extension.cpp)
add_library(icu_extension icu-collate.cpp icu-extension.cpp)

add_subdirectory(test)
