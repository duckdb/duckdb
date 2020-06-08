cmake_minimum_required(VERSION 2.8.7)

project(ICUExtension)

include_directories(include)

add_library(icu_extension STATIC icu-collate.cpp icu-extension.cpp)

add_subdirectory(test)
