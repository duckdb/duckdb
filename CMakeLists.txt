cmake_minimum_required(VERSION 2.8.7)
project(DuckDB)


find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_VERBOSE_MAKEFILE on)


set(CMAKE_CXX_FLAGS_DEFAULT "-O0 -g")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -fsanitize=address -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -O3")

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_C_FLAGS_DEFAULT "${CMAKE_CXX_FLAGS_DEFAULT}")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")

if (NOT CMAKE_BUILD_TYPE)
    message(FATAL_ERROR "Choose the type of build by setting -DCMAKE_BUILD_TYPE=<type>, options are: Debug Release RelWithDebInfo MinSizeRel.")
endif()

include_directories(src/include)

add_subdirectory(src)
add_subdirectory(third_party)
add_subdirectory(test)
