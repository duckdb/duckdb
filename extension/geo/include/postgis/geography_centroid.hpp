#pragma once
#include "duckdb.hpp"
#include "liblwgeom/liblwgeom.hpp"

namespace duckdb {

GSERIALIZED *geography_centroid(GSERIALIZED *geom, bool use_spheroid);

} // namespace duckdb
