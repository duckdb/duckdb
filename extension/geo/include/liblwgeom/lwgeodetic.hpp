#pragma once
#include "liblwgeom/liblwgeom.hpp"

namespace duckdb {

int edge_calculate_gbox(const POINT3D *A1, const POINT3D *A2, GBOX *gbox);
void vector_sum(const POINT3D *a, const POINT3D *b, POINT3D *n);
void normalize(POINT3D *p);
void unit_normal(const POINT3D *P1, const POINT3D *P2, POINT3D *normal);
void ll2cart(const POINT2D *g, POINT3D *p);

} // namespace duckdb
