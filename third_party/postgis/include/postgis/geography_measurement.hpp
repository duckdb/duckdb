#pragma once

#include "liblwgeom/liblwgeom.hpp"

namespace duckdb_postgis {

double geography_distance(GSERIALIZED *geom1, GSERIALIZED *geom2, bool use_spheroid);

int geography_tree_distance(const GSERIALIZED* g1, const GSERIALIZED* g2, const SPHEROID* s, double tolerance, double* distance);

} // duckdb_postgis