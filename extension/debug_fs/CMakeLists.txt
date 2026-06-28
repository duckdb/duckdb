cmake_minimum_required(VERSION 3.5...3.29)

project(DebugFsExtension)

include_directories(include)

set(DEBUG_FS_FILES
    debug_fs_extension.cpp
    debug_file_system.cpp
    io_latency_model.cpp)

build_static_extension(debug_fs ${DEBUG_FS_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(debug_fs ${PARAMETERS} ${DEBUG_FS_FILES})
