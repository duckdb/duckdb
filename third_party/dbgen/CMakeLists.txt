cmake_minimum_required(VERSION 2.8.7)

project(dbgen CXX C)

include_directories(include)

add_definitions(-D_FILE_OFFSET_BITS=64 -DDBNAME=dss -DMAC -DORACLE -DTPCH)

add_library(dbgen STATIC bm_utils.c build.c dbgen.cpp dbgen_gunk.cpp permute.c rnd.c rng64.c speed_seed.c text.c)
