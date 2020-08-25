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
    bm_utils.c
    build.c
    permute.c
    rnd.c
    rng64.c
    speed_seed.c
    text.c)

set_property(TARGET dbgen PROPERTY C_STANDARD 99)

disable_target_warnings(dbgen)
