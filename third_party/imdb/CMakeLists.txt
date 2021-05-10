cmake_minimum_required(VERSION 2.8.12)

if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

project(imdb CXX)

include_directories(include)

set(CMAKE_BUILD_TYPE "Release")
add_library(imdb STATIC imdb.cpp ${ALL_OBJECT_FILES})

disable_target_warnings(imdb)
