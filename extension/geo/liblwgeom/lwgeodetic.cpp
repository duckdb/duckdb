#include "liblwgeom/lwgeodetic.hpp"

#include "liblwgeom/liblwgeom_internal.hpp"
#include "liblwgeom/lwinline.hpp"

#include <cassert>
#include <cstring>
#include <math.h>

namespace duckdb {

/**
 * Calculate the dot product of two unit vectors
 * (-1 == opposite, 0 == orthogonal, 1 == identical)
 */
static double dot_product(const POINT3D *p1, const POINT3D *p2) {
	return (p1->x * p2->x) + (p1->y * p2->y) + (p1->z * p2->z);
}

/**
 * Calculate the cross product of two vectors
 */
static void cross_product(const POINT3D *a, const POINT3D *b, POINT3D *n) {
	n->x = a->y * b->z - a->z * b->y;
	n->y = a->z * b->x - a->x * b->z;
	n->z = a->x * b->y - a->y * b->x;
	return;
}

/**
 * Calculate the difference of two vectors
 */
static void vector_difference(const POINT3D *a, const POINT3D *b, POINT3D *n) {
	n->x = a->x - b->x;
	n->y = a->y - b->y;
	n->z = a->z - b->z;
	return;
}

/**
 * Calculate the sum of two vectors
 */
void vector_sum(const POINT3D *a, const POINT3D *b, POINT3D *n) {
	n->x = a->x + b->x;
	n->y = a->y + b->y;
	n->z = a->z + b->z;
	return;
}

/**
 * Normalize to a unit vector.
 */
void normalize(POINT3D *p) {
	double d = sqrt(p->x * p->x + p->y * p->y + p->z * p->z);
	if (FP_IS_ZERO(d)) {
		p->x = p->y = p->z = 0.0;
		return;
	}
	p->x = p->x / d;
	p->y = p->y / d;
	p->z = p->z / d;
	return;
}

/**
 * Normalize to a unit vector.
 */
static void normalize2d(POINT2D *p) {
	double d = sqrt(p->x * p->x + p->y * p->y);
	if (FP_IS_ZERO(d)) {
		p->x = p->y = 0.0;
		return;
	}
	p->x = p->x / d;
	p->y = p->y / d;
	return;
}

/**
 * Calculates the unit normal to two vectors, trying to avoid
 * problems with over-narrow or over-wide cases.
 */
void unit_normal(const POINT3D *P1, const POINT3D *P2, POINT3D *normal) {
	double p_dot = dot_product(P1, P2);
	POINT3D P3;

	/* If edge is really large, calculate a narrower equivalent angle A1/A3. */
	if (p_dot < 0) {
		vector_sum(P1, P2, &P3);
		normalize(&P3);
	}
	/* If edge is narrow, calculate a wider equivalent angle A1/A3. */
	else if (p_dot > 0.95) {
		vector_difference(P2, P1, &P3);
		normalize(&P3);
	}
	/* Just keep the current angle in A1/A3. */
	else {
		P3 = *P2;
	}

	/* Normals to the A-plane and B-plane */
	cross_product(P1, &P3, normal);
	normalize(normal);
}

/**
 * The magic function, given an edge in spherical coordinates, calculate a
 * 3D bounding box that fully contains it, taking into account the curvature
 * of the sphere on which it is inscribed.
 *
 * Any arc on the sphere defines a plane that bisects the sphere. In this plane,
 * the arc is a portion of a unit circle.
 * Projecting the end points of the axes (1,0,0), (-1,0,0) etc, into the plane
 * and normalizing yields potential extrema points. Those points on the
 * side of the plane-dividing line formed by the end points that is opposite
 * the origin of the plane are extrema and should be added to the bounding box.
 */
int edge_calculate_gbox(const POINT3D *A1, const POINT3D *A2, GBOX *gbox) {
	POINT2D R1, R2, RX, O;
	POINT3D AN, A3;
	POINT3D X[6];
	int i, o_side;

	/* Initialize the box with the edge end points */
	gbox_init_point3d(A1, gbox);
	gbox_merge_point3d(A2, gbox);

	/* Zero length edge, just return! */
	if (p3d_same(A1, A2))
		return LW_SUCCESS;

	/* Error out on antipodal edge */
	if (FP_EQUALS(A1->x, -1 * A2->x) && FP_EQUALS(A1->y, -1 * A2->y) && FP_EQUALS(A1->z, -1 * A2->z)) {
		// lwerror("Antipodal (180 degrees long) edge detected!");
		return LW_FAILURE;
	}

	/* Create A3, a vector in the plane of A1/A2, orthogonal to A1  */
	unit_normal(A1, A2, &AN);
	unit_normal(&AN, A1, &A3);

	/* Project A1 and A2 into the 2-space formed by the plane A1/A3 */
	R1.x = 1.0;
	R1.y = 0.0;
	R2.x = dot_product(A2, A1);
	R2.y = dot_product(A2, &A3);

	/* Initialize our 3-space axis points (x+, x-, y+, y-, z+, z-) */
	memset(X, 0, sizeof(POINT3D) * 6);
	X[0].x = X[2].y = X[4].z = 1.0;
	X[1].x = X[3].y = X[5].z = -1.0;

	/* Initialize a 2-space origin point. */
	O.x = O.y = 0.0;
	/* What side of the line joining R1/R2 is O? */
	o_side = lw_segment_side(&R1, &R2, &O);

	/* Add any extrema! */
	for (i = 0; i < 6; i++) {
		/* Convert 3-space axis points to 2-space unit vectors */
		RX.x = dot_product(&(X[i]), A1);
		RX.y = dot_product(&(X[i]), &A3);
		normalize2d(&RX);

		/* Any axis end on the side of R1/R2 opposite the origin */
		/* is an extreme point in the arc, so we add the 3-space */
		/* version of the point on R1/R2 to the gbox */
		if (lw_segment_side(&R1, &R2, &RX) != o_side) {
			POINT3D Xn;
			Xn.x = RX.x * A1->x + RX.y * A3.x;
			Xn.y = RX.x * A1->y + RX.y * A3.y;
			Xn.z = RX.x * A1->z + RX.y * A3.z;

			gbox_merge_point3d(&Xn, gbox);
		}
	}

	return LW_SUCCESS;
}

/**
 * Convert lon/lat coordinates to cartesian coordinates on unit sphere
 */
void ll2cart(const POINT2D *g, POINT3D *p) {
	double x_rad = M_PI * g->x / 180.0;
	double y_rad = M_PI * g->y / 180.0;
	double cos_y_rad = cos(y_rad);
	p->x = cos_y_rad * cos(x_rad);
	p->y = cos_y_rad * sin(x_rad);
	p->z = sin(y_rad);
}

int ptarray_calculate_gbox_geodetic(const POINTARRAY *pa, GBOX *gbox) {
	uint32_t i;
	int first = LW_TRUE;
	const POINT2D *p;
	POINT3D A1, A2;
	GBOX edge_gbox;

	assert(gbox);
	assert(pa);

	gbox_init(&edge_gbox);
	edge_gbox.flags = gbox->flags;

	if (pa->npoints == 0)
		return LW_FAILURE;

	if (pa->npoints == 1) {
		p = getPoint2d_cp(pa, 0);
		ll2cart(p, &A1);
		gbox->xmin = gbox->xmax = A1.x;
		gbox->ymin = gbox->ymax = A1.y;
		gbox->zmin = gbox->zmax = A1.z;
		return LW_SUCCESS;
	}

	p = getPoint2d_cp(pa, 0);
	ll2cart(p, &A1);

	for (i = 1; i < pa->npoints; i++) {

		p = getPoint2d_cp(pa, i);
		ll2cart(p, &A2);

		edge_calculate_gbox(&A1, &A2, &edge_gbox);

		/* Initialize the box */
		if (first) {
			gbox_duplicate(&edge_gbox, gbox);
			first = LW_FALSE;
		}
		/* Expand the box where necessary */
		else {
			gbox_merge(&edge_gbox, gbox);
		}

		A1 = A2;
	}

	return LW_SUCCESS;
}

static int lwpoint_calculate_gbox_geodetic(const LWPOINT *point, GBOX *gbox) {
	assert(point);
	return ptarray_calculate_gbox_geodetic(point->point, gbox);
}
int lwgeom_calculate_gbox_geodetic(const LWGEOM *geom, GBOX *gbox) {
	int result = LW_FAILURE;

	/* Add a geodetic flag to the incoming gbox */
	gbox->flags = lwflags(FLAGS_GET_Z(geom->flags), FLAGS_GET_M(geom->flags), 1);

	switch (geom->type) {
	case POINTTYPE:
		result = lwpoint_calculate_gbox_geodetic((LWPOINT *)geom, gbox);
		break;
	default:
		// lwerror("lwgeom_calculate_gbox_geodetic: unsupported input geometry type: %d - %s",
		//         geom->type, lwtype_name(geom->type));
		break;
	}
	return result;
}

} // namespace duckdb
