#pragma once

#include "liblwgeom/liblwgeom.hpp"

namespace duckdb_postgis {

GSERIALIZED *LWGEOM_makepoint(double x, double y);
GSERIALIZED *LWGEOM_makepoint(double x, double y, double z);
double ST_distance(GSERIALIZED *geom1, GSERIALIZED *geom2);

}