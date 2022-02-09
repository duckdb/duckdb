#pragma once

#include "liblwgeom/liblwgeom.hpp"

namespace duckdb_postgis {

GSERIALIZED* LWGEOM_from_text(char* text, int srid = SRID_UNKNOWN);
GSERIALIZED* LWGEOM_from_WKB(char *bytea_wkb, int srid = SRID_UNKNOWN);
double LWGEOM_x_point(const void* base, size_t size);

} // duckdb_postgis