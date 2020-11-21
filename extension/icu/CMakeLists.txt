cmake_minimum_required(VERSION 2.8.12)

project(ICUExtension)

include_directories(include)

add_library(icu_extension STATIC icu-collate.cpp icu-extension.cpp)
target_link_libraries(icu_extension Threads::Threads)
disable_target_warnings(icu_extension)
