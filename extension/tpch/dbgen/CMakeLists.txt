cmake_minimum_required(VERSION 2.8.7)

project(dbgen CXX C)

include_directories(include)

add_definitions(-DDBNAME=dss
                -DMAC
                -DORACLE
                -DTPCH)

add_library(dbgen STATIC
    dbgen.cpp
    dbgen_gunk.cpp
    bm_utils.cpp
    build.cpp
    permute.cpp
    rnd.cpp
    rng64.cpp
    speed_seed.cpp
    text.cpp)

disable_target_warnings(dbgen)
