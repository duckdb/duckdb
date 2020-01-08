cmake_minimum_required(VERSION 2.8.7)

project(dbgen CXX C)
cmake_policy(SET CMP0063 NEW)

include_directories(include)

add_definitions(-D_FILE_OFFSET_BITS=64
                -DDBNAME=dss
                -DMAC
                -DORACLE
                -DTPCH)

set(CMAKE_BUILD_TYPE "Release")
add_library(dbgen STATIC
            bm_utils.c
            build.c
            dbgen.cpp
            dbgen_gunk.cpp
            permute.c
            rnd.c
            rng64.c
            speed_seed.c
            text.c)

set_property(TARGET dbgen PROPERTY C_STANDARD 99)

disable_target_warnings(dbgen)
