cmake_minimum_required(VERSION 2.8.12)

project(GEOExtension)

include_directories(include)
include_directories(third_party/json/include)

add_subdirectory(third_party)

set(GEO_EXTENSION_FILES
    ${GEO_LIBRARY_FILES}
    geo-extension.cpp
    geo-functions.cpp
    postgis.cpp
    geometry.cpp
    postgis/lwgeom_inout.cpp
    liblwgeom/lwin_wkt.cpp
    liblwgeom/lwin_wkb.cpp
    liblwgeom/lwutil.cpp
    liblwgeom/ptarray.cpp
    liblwgeom/lwpoint.cpp
    liblwgeom/lwgeom.cpp
    liblwgeom/gbox.cpp
    liblwgeom/lwout_wkb.cpp
    liblwgeom/lwgeodetic.cpp
    liblwgeom/lwalgorithm.cpp
    liblwgeom/gserialized.cpp
    liblwgeom/gserialized1.cpp
    liblwgeom/gserialized2.cpp
    liblwgeom/lwgeom_api.cpp
    liblwgeom/lwout_wkt.cpp
    liblwgeom/stringbuffer.cpp
    liblwgeom/lwprint.cpp
    liblwgeom/lwin_geojson.cpp
    liblwgeom/lwout_geojson.cpp
    parser/lwin_wkt_lex.cpp
    parser/lwin_wkt_parse.cpp
    libpgcommon/lwgeom_pg.cpp
    lib/ryu.cpp)

add_library(geo_extension STATIC ${GEO_EXTENSION_FILES})
set(INGORE_WARNINGS OFF)
build_loadable_extension(geo ${GEO_EXTENSION_FILES} ${IGNORE_WARNINGS})

install(
  TARGETS geo_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
