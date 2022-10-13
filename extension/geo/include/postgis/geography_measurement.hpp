#pragma once

#include "liblwgeom/liblwgeom.hpp"

namespace duckdb {

#ifndef _LIBGEOGRAPHY_MEASUREMENT_H
#define _LIBGEOGRAPHY_MEASUREMENT_H 1

double geography_distance(GSERIALIZED *geom1, GSERIALIZED *geom2, bool use_spheroid);

int geography_tree_distance(const GSERIALIZED *g1, const GSERIALIZED *g2, const SPHEROID *s, double tolerance,
                            double *distance);

#endif /* !defined _LIBGEOGRAPHY_MEASUREMENT_H  */

} // namespace duckdb
