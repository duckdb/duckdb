cmake_minimum_required(VERSION 2.8.12)

project(duckdb_numformat CXX C)
if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

add_library(duckdb_numformat STATIC
            localedata.cpp
            calendar.cpp
            calendar.h
            CMakeLists.txt
            date.hxx
            datetime.cxx
            datetime.hxx
            define.h
            digitgroupingiterator.hxx
            LICENSE
            nfkeytab.hxx
            nfsymbol.hxx
            README.md
            tdate.cxx
            time.hxx
            ttime.cxx
            zforfind.cxx
            zforfind.hxx
            zformat.cxx
            zformat.hxx
            zforscan.cxx
            zforscan.hxx)           

target_include_directories(
  duckdb_numformat
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
set_target_properties(duckdb_numformat PROPERTIES EXPORT_NAME duckdb_duckdb_numformat)

install(TARGETS duckdb_numformat
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(duckdb_numformat)

set_target_properties(duckdb_numformat PROPERTIES POSITION_INDEPENDENT_CODE ON)
