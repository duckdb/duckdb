cmake_minimum_required(VERSION 2.8.7)

project(FTSExtension)

include_directories(fts5/include)
include_directories(include)
add_subdirectory(fts5)

add_library(
  fts_extension STATIC
  fts-extension.cpp
  fts_stemmer.cpp
  )
target_link_libraries(fts_extension fts5)

