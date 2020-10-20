cmake_minimum_required(VERSION 2.8.7)

project(fts5 CXX C)

include_directories(include)

add_library(
  fts5 STATIC
  fts5_tokenize.c)

disable_target_warnings(fts5)
