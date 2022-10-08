#include "liblwgeom/liblwgeom_internal.hpp"

namespace duckdb {

int p3d_same(const POINT3D *p1, const POINT3D *p2) {
	if (FP_EQUALS(p1->x, p2->x) && FP_EQUALS(p1->y, p2->y) && FP_EQUALS(p1->z, p2->z))
		return LW_TRUE;
	else
		return LW_FALSE;
}

/**
 * lw_segment_side()
 *
 * Return -1  if point Q is left of segment P
 * Return  1  if point Q is right of segment P
 * Return  0  if point Q in on segment P
 */
int lw_segment_side(const POINT2D *p1, const POINT2D *p2, const POINT2D *q) {
	double side = ((q->x - p1->x) * (p2->y - p1->y) - (p2->x - p1->x) * (q->y - p1->y));
	return SIGNUM(side);
}

} // namespace duckdb
