#include "liblwgeom/measures.hpp"

#include "liblwgeom/liblwgeom.hpp"
#include "liblwgeom/lwinline.hpp"

namespace duckdb {

/**
    This function just deserializes geometries
    Bboxes is not checked here since it is the subgeometries
    bboxes we will use anyway.
*/
int lw_dist2d_comp(const LWGEOM *lw1, const LWGEOM *lw2, DISTPTS *dl) {
	return lw_dist2d_recursive(lw1, lw2, dl);
}

int lw_dist2d_distribute_bruteforce(const LWGEOM *lwg1, const LWGEOM *lwg2, DISTPTS *dl) {

	int t1 = lwg1->type;
	int t2 = lwg2->type;

	switch (t1) {
	case POINTTYPE: {
		dl->twisted = 1;
		switch (t2) {
		case POINTTYPE:
			return lw_dist2d_point_point((LWPOINT *)lwg1, (LWPOINT *)lwg2, dl);
			// Need to do with postgis

		default:
			lwerror("%s: Unsupported geometry type: %s", __func__, lwtype_name(t2));
			return LW_FALSE;
		}
	}
		// Need to do with postgis

	default: {
		lwerror("%s: Unsupported geometry type: %s", __func__, lwtype_name(t1));
		return LW_FALSE;
	}
	}

	return LW_FALSE;
}

/**
This is a recursive function delivering every possible combination of subgeometries
*/
int lw_dist2d_recursive(const LWGEOM *lwg1, const LWGEOM *lwg2, DISTPTS *dl) {
	// Need to do with postgis

	int i, j;
	int n1 = 1;
	int n2 = 1;
	LWGEOM *g1 = NULL;
	LWGEOM *g2 = NULL;
	// LWCOLLECTION *c1 = NULL;
	// LWCOLLECTION *c2 = NULL;

	// if (lw_dist2d_is_collection(lwg1))
	// {
	// 	c1 = lwgeom_as_lwcollection(lwg1);
	// 	n1 = c1->ngeoms;
	// }
	// if (lw_dist2d_is_collection(lwg2))
	// {
	// 	c2 = lwgeom_as_lwcollection(lwg2);
	// 	n2 = c2->ngeoms;
	// }

	for (i = 0; i < n1; i++) {

		// if (lw_dist2d_is_collection(lwg1))
		// 	g1 = c1->geoms[i];
		// else
		g1 = (LWGEOM *)lwg1;

		if (lwgeom_is_empty(g1))
			return LW_TRUE;

		// if (lw_dist2d_is_collection(g1))
		// {
		// 	if (!lw_dist2d_recursive(g1, lwg2, dl))
		// 		return LW_FALSE;
		// 	continue;
		// }
		for (j = 0; j < n2; j++) {
			// if (lw_dist2d_is_collection(lwg2))
			// 	g2 = c2->geoms[j];
			// else
			g2 = (LWGEOM *)lwg2;

			// if (lw_dist2d_is_collection(g2))
			// {
			// 	if (!lw_dist2d_recursive(g1, g2, dl))
			// 		return LW_FALSE;
			// 	continue;
			// }

			if (!g1->bbox)
				lwgeom_add_bbox(g1);

			if (!g2->bbox)
				lwgeom_add_bbox(g2);

			/* If one of geometries is empty, return. True here only means continue searching. False would
			 * have stopped the process*/
			if (lwgeom_is_empty(g1) || lwgeom_is_empty(g2))
				return LW_TRUE;

			// if ((dl->mode != DIST_MAX) && (!lw_dist2d_check_overlap(g1, g2)) &&
			//     (g1->type == LINETYPE || g1->type == POLYGONTYPE || g1->type == TRIANGLETYPE) &&
			//     (g2->type == LINETYPE || g2->type == POLYGONTYPE || g2->type == TRIANGLETYPE))
			// {
			// 	if (!lw_dist2d_distribute_fast(g1, g2, dl))
			// 		return LW_FALSE;
			// }
			// else
			{
				if (!lw_dist2d_distribute_bruteforce(g1, g2, dl))
					return LW_FALSE;
				if (dl->distance <= dl->tolerance && dl->mode == DIST_MIN)
					return LW_TRUE; /*just a check if the answer is already given*/
			}
		}
	}
	return LW_TRUE;
}

/**
Function initializing min distance calculation
*/
double lwgeom_mindistance2d(const LWGEOM *lw1, const LWGEOM *lw2) {
	return lwgeom_mindistance2d_tolerance(lw1, lw2, 0.0);
}

/**
    Function handling min distance calculations and dwithin calculations.
    The difference is just the tolerance.
*/
double lwgeom_mindistance2d_tolerance(const LWGEOM *lw1, const LWGEOM *lw2, double tolerance) {
	DISTPTS thedl;
	thedl.mode = DIST_MIN;
	thedl.distance = FLT_MAX;
	thedl.tolerance = tolerance;
	if (lw_dist2d_comp(lw1, lw2, &thedl))
		return thedl.distance;
	/*should never get here. all cases ought to be error handled earlier*/
	lwerror("Some unspecified error.");
	return FLT_MAX;
}

/** Compares incoming points and stores the points closest to each other or most far away from each other depending on
 * dl->mode (max or min) */
int lw_dist2d_pt_pt(const POINT2D *thep1, const POINT2D *thep2, DISTPTS *dl) {
	double hside = thep2->x - thep1->x;
	double vside = thep2->y - thep1->y;
	double dist = sqrt(hside * hside + vside * vside);

	/*multiplication with mode to handle mindistance (mode=1) and maxdistance (mode = (-1)*/
	if (((dl->distance - dist) * (dl->mode)) > 0) {
		dl->distance = dist;

		/* To get the points in right order. twisted is updated between 1 and (-1) every time the order is
		 * changed earlier in the chain*/
		if (dl->twisted > 0) {
			dl->p1 = *thep1;
			dl->p2 = *thep2;
		} else {
			dl->p1 = *thep2;
			dl->p2 = *thep1;
		}
	}
	return LW_TRUE;
}

/*------------------------------------------------------------------------------------------------------------
Brute force functions
The old way of calculating distances, now used for:
1)	distances to points (because there shouldn't be anything to gain by the new way of doing it)
2)	distances when subgeometries geometries bboxes overlaps
--------------------------------------------------------------------------------------------------------------*/

/**
point to point calculation
*/
int lw_dist2d_point_point(LWPOINT *point1, LWPOINT *point2, DISTPTS *dl) {
	const POINT2D *p1 = getPoint2d_cp(point1->point, 0);
	const POINT2D *p2 = getPoint2d_cp(point2->point, 0);
	return lw_dist2d_pt_pt(p1, p2, dl);
}

} // namespace duckdb
