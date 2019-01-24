cmake_minimum_required(VERSION 2.8.7)
project(DuckDB)

find_package(Threads REQUIRED)


set(CMAKE_CXX_STANDARD 11)
set(CMAKE_VERBOSE_MAKEFILE off)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_MACOSX_RPATH 1)

set(CMAKE_CXX_FLAGS_DEFAULT "-O0 -g")
IF(NOT MSVC AND NOT $ENV{TRAVIS_BUILD}==1)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -fsanitize=address -DDEBUG -Wall -Wunused-variable -Wunused-const-variable")
ENDIF()

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -O3")

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_C_FLAGS_DEFAULT "${CMAKE_CXX_FLAGS_DEFAULT}")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")

IF (NOT CMAKE_BUILD_TYPE)
    message(FATAL_ERROR "Choose the type of build by setting -DCMAKE_BUILD_TYPE=<type>, options are: Debug Release RelWithDebInfo MinSizeRel.")
ENDIF()

include_directories(src/include)
include_directories(third_party/hyperloglog)

add_subdirectory(src)
add_subdirectory(third_party)
add_subdirectory(test)

IF (NOT WIN32)
  add_subdirectory(tools)
  add_subdirectory(benchmark)
ENDIF()
