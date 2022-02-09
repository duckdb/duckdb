# cmake_minimum_required(VERSION 2.8.12)
cmake_minimum_required(VERSION 3.14)

project(postgis CXX C)
if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

include_directories(json/include)

add_subdirectory(json)

add_library(postgis STATIC
            ${POSTGIS_OBJECT_FILES}
            postgis.cpp
            postgis/lwgeom_inout.cpp
            postgis/lwgeom_functions_basic.cpp
            postgis/lwgeom_ogc.cpp
            postgis/geography_measurement.cpp
            libpgcommon/lwgeom_pg.cpp
            libpgcommon/lwgeom_transform.cpp
            liblwgeom/stringbuffer.cpp
            liblwgeom/lwin_wkt.cpp
            liblwgeom/lwout_wkt.cpp
            liblwgeom/lwin_wkb.cpp
            liblwgeom/lwout_wkb.cpp
            liblwgeom/lwgeom.cpp
            liblwgeom/lwutil.cpp
            liblwgeom/ptarray.cpp
            liblwgeom/lwpoint.cpp
            liblwgeom/gbox.cpp
            liblwgeom/lwgeodetic.cpp
            liblwgeom/gserialized.cpp
            liblwgeom/gserialized1.cpp
            liblwgeom/gserialized2.cpp
            liblwgeom/lwalgorithm.cpp
            liblwgeom/lwgeom_api.cpp
            liblwgeom/lwin_wkt_lex.cpp
            liblwgeom/lwin_wkt_parse.cpp
            liblwgeom/lwprint.cpp
            liblwgeom/lwin_geojson.cpp
            liblwgeom/lwout_geojson.cpp
            liblwgeom/measures.cpp
            liblwgeom/lwspheroid.cpp
            liblwgeom/lwgeodetic_tree.cpp
            lib/ryu.cpp)

target_include_directories(
  postgis
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
set_target_properties(postgis PROPERTIES EXPORT_NAME duckdb_postgis)

install(TARGETS postgis
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(postgis)