cmake_minimum_required(VERSION 2.8.12)

project(TPCDSExtension)

include_directories(include)
include_directories(include/dsdgen)
add_subdirectory(dsdgen)

add_library(tpcds_extension STATIC tpcds-extension.cpp ${DSDGEN_OBJECT_FILES})
# target_link_libraries(tpcds_extension dsdgen)
