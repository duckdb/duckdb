#include "liblwgeom/lwgeodetic.hpp"

#include "liblwgeom/liblwgeom_internal.hpp"
#include "liblwgeom/lwinline.hpp"

#include <cassert>
#include <cstring>
#include <math.h>

namespace duckdb {

/**
 * Utility function for ptarray_contains_point_sphere()
 */
static int point3d_equals(const POINT3D *p1, const POINT3D *p2) {
	return FP_EQUALS(p1->x, p2->x) && FP_EQUALS(p1->y, p2->y) && FP_EQUALS(p1->z, p2->z);
}

/**
 * Calculate the dot product of two unit vectors
 * (-1 == opposite, 0 == orthogonal, 1 == identical)
 */
static double dot_product(const POINT3D *p1, const POINT3D *p2) {
	return (p1->x * p2->x) + (p1->y * p2->y) + (p1->z * p2->z);
}

/**
 * Utility function for edge_intersects(), signum with a tolerance
 * in determining if the value is zero.
 */
static int dot_product_side(const POINT3D *p, const POINT3D *q) {
	double dp = dot_product(p, q);

	if (FP_IS_ZERO(dp))
		return 0;

	return dp < 0.0 ? -1 : 1;
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
 * Convert spherical coordinates to cartesian coordinates on unit sphere
 */
void geog2cart(const GEOGRAPHIC_POINT *g, POINT3D *p) {
	p->x = cos(g->lat) * cos(g->lon);
	p->y = cos(g->lat) * sin(g->lon);
	p->z = sin(g->lat);
}

/**
 * Convert cartesian coordinates on unit sphere to spherical coordinates
 */
void cart2geog(const POINT3D *p, GEOGRAPHIC_POINT *g) {
	g->lon = atan2(p->y, p->x);
	g->lat = asin(p->z);
}

/**
 * Computes the cross product of two vectors using their lat, lng representations.
 * Good even for small distances between p and q.
 */
void robust_cross_product(const GEOGRAPHIC_POINT *p, const GEOGRAPHIC_POINT *q, POINT3D *a) {
	double lon_qpp = (q->lon + p->lon) / -2.0;
	double lon_qmp = (q->lon - p->lon) / 2.0;
	double sin_p_lat_minus_q_lat = sin(p->lat - q->lat);
	double sin_p_lat_plus_q_lat = sin(p->lat + q->lat);
	double sin_lon_qpp = sin(lon_qpp);
	double sin_lon_qmp = sin(lon_qmp);
	double cos_lon_qpp = cos(lon_qpp);
	double cos_lon_qmp = cos(lon_qmp);
	a->x = sin_p_lat_minus_q_lat * sin_lon_qpp * cos_lon_qmp - sin_p_lat_plus_q_lat * cos_lon_qpp * sin_lon_qmp;
	a->y = sin_p_lat_minus_q_lat * cos_lon_qpp * cos_lon_qmp + sin_p_lat_plus_q_lat * sin_lon_qpp * sin_lon_qmp;
	a->z = cos(p->lat) * cos(q->lat) * sin(q->lon - p->lon);
}

/**
 * Given two points on a unit sphere, calculate the direction from s to e.
 */
double sphere_direction(const GEOGRAPHIC_POINT *s, const GEOGRAPHIC_POINT *e, double d) {
	double heading = 0.0;
	double f;

	/* Starting from the poles? Special case. */
	if (FP_IS_ZERO(cos(s->lat)))
		return (s->lat > 0.0) ? M_PI : 0.0;

	f = (sin(e->lat) - sin(s->lat) * cos(d)) / (sin(d) * cos(s->lat));
	if (FP_EQUALS(f, 1.0))
		heading = 0.0;
	else if (FP_EQUALS(f, -1.0))
		heading = M_PI;
	else if (fabs(f) > 1.0) {
		heading = acos(f);
	} else
		heading = acos(f);

	if (sin(e->lon - s->lon) < 0.0)
		heading = -1 * heading;

	return heading;
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
 * Returns -1 if the point is to the left of the plane formed
 * by the edge, 1 if the point is to the right, and 0 if the
 * point is on the plane.
 */
static int edge_point_side(const GEOGRAPHIC_EDGE *e, const GEOGRAPHIC_POINT *p) {
	POINT3D normal, pt;
	double w;
	/* Normal to the plane defined by e */
	robust_cross_product(&(e->start), &(e->end), &normal);
	normalize(&normal);
	geog2cart(p, &pt);
	/* We expect the dot product of with normal with any vector in the plane to be zero */
	w = dot_product(&normal, &pt);
	if (FP_IS_ZERO(w)) {
		return 0;
	}

	if (w < 0)
		return -1;
	else
		return 1;
}

/**
 * Utility function for checking if P is within the cone defined by A1/A2.
 */
static int point_in_cone(const POINT3D *A1, const POINT3D *A2, const POINT3D *P) {
	POINT3D AC; /* Center point of A1/A2 */
	double min_similarity, similarity;

	/* Boundary case */
	if (point3d_equals(A1, P) || point3d_equals(A2, P))
		return LW_TRUE;

	/* The normalized sum bisects the angle between start and end. */
	vector_sum(A1, A2, &AC);
	normalize(&AC);

	/* The projection of start onto the center defines the minimum similarity */
	min_similarity = dot_product(A1, &AC);

	/* If the edge is sufficiently curved, use the dot product test */
	if (fabs(1.0 - min_similarity) > 1e-10) {
		/* The projection of candidate p onto the center */
		similarity = dot_product(P, &AC);

		/* If the projection of the candidate is larger than */
		/* the projection of the start point, the candidate */
		/* must be closer to the center than the start, so */
		/* therefor inside the cone */
		if (similarity > min_similarity) {
			return LW_TRUE;
		} else {
			return LW_FALSE;
		}
	} else {
		/* Where the edge is very narrow, the dot product test */
		/* fails, but we can use the almost-planar nature of the */
		/* problem space then to test if the vector from the */
		/* candidate to the start point in a different direction */
		/* to the vector from candidate to end point */
		/* If so, then candidate is between start and end */
		POINT3D PA1, PA2;
		vector_difference(P, A1, &PA1);
		vector_difference(P, A2, &PA2);
		normalize(&PA1);
		normalize(&PA2);
		if (dot_product(&PA1, &PA2) < 0.0) {
			return LW_TRUE;
		} else {
			return LW_FALSE;
		}
	}
	return LW_FALSE;
}

/**
 * Returns true if the point p is on the great circle plane.
 * Forms the scalar triple product of A,B,p and if the volume of the
 * resulting parallelepiped is near zero the point p is on the
 * great circle plane.
 */
int edge_point_on_plane(const GEOGRAPHIC_EDGE *e, const GEOGRAPHIC_POINT *p) {
	int side = edge_point_side(e, p);
	if (side == 0)
		return LW_TRUE;

	return LW_FALSE;
}

/**
 * Returns true if the point p is inside the cone defined by the
 * two ends of the edge e.
 */
int edge_point_in_cone(const GEOGRAPHIC_EDGE *e, const GEOGRAPHIC_POINT *p) {
	POINT3D vcp, vs, ve, vp;
	double vs_dot_vcp, vp_dot_vcp;
	geog2cart(&(e->start), &vs);
	geog2cart(&(e->end), &ve);
	/* Antipodal case, everything is inside. */
	if (vs.x == -1.0 * ve.x && vs.y == -1.0 * ve.y && vs.z == -1.0 * ve.z)
		return LW_TRUE;
	geog2cart(p, &vp);
	/* The normalized sum bisects the angle between start and end. */
	vector_sum(&vs, &ve, &vcp);
	normalize(&vcp);
	/* The projection of start onto the center defines the minimum similarity */
	vs_dot_vcp = dot_product(&vs, &vcp);
	/* The projection of candidate p onto the center */
	vp_dot_vcp = dot_product(&vp, &vcp);

	/*
	** We want to test that vp_dot_vcp is >= vs_dot_vcp but there are
	** numerical stability issues for values that are very very nearly
	** equal. Unfortunately there are also values of vp_dot_vcp that are legitimately
	** very close to but still less than vs_dot_vcp which we also need to catch.
	** The tolerance of 10-17 seems to do the trick on 32-bit and 64-bit architectures,
	** for the test cases here.
	** However, tuning the tolerance value feels like a dangerous hack.
	** Fundamentally, the problem is that this test is so sensitive.
	*/

	/* 1.1102230246251565404236316680908203125e-16 */

	if (vp_dot_vcp > vs_dot_vcp || fabs(vp_dot_vcp - vs_dot_vcp) < 2e-16) {
		return LW_TRUE;
	}
	return LW_FALSE;
}

/**
 * Scale a vector out by a factor
 */
void vector_scale(POINT3D *n, double scale) {
	n->x *= scale;
	n->y *= scale;
	n->z *= scale;
	return;
}

/**
 * Returns true if the point p is on the minor edge defined by the
 * end points of e.
 */
int edge_contains_point(const GEOGRAPHIC_EDGE *e, const GEOGRAPHIC_POINT *p) {
	if (edge_point_in_cone(e, p) && edge_point_on_plane(e, p))
	/*	if ( edge_contains_coplanar_point(e, p) && edge_point_on_plane(e, p) ) */
	{
		return LW_TRUE;
	}
	return LW_FALSE;
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

/**
 * Initialize a geographic point
 * @param lon longitude in degrees
 * @param lat latitude in degrees
 */
void geographic_point_init(double lon, double lat, GEOGRAPHIC_POINT *g) {
	g->lat = latitude_radians_normalize(deg2rad(lat));
	g->lon = longitude_radians_normalize(deg2rad(lon));
}

/**
 * Convert a longitude to the range of -PI,PI
 */
double longitude_radians_normalize(double lon) {
	if (lon == -1.0 * M_PI)
		return M_PI;
	if (lon == -2.0 * M_PI)
		return 0.0;

	if (lon > 2.0 * M_PI)
		lon = remainder(lon, 2.0 * M_PI);

	if (lon < -2.0 * M_PI)
		lon = remainder(lon, -2.0 * M_PI);

	if (lon > M_PI)
		lon = -2.0 * M_PI + lon;

	if (lon < -1.0 * M_PI)
		lon = 2.0 * M_PI + lon;

	if (lon == -2.0 * M_PI)
		lon *= -1.0;

	return lon;
}

/**
 * Convert a latitude to the range of -PI/2,PI/2
 */
double latitude_radians_normalize(double lat) {

	if (lat > 2.0 * M_PI)
		lat = remainder(lat, 2.0 * M_PI);

	if (lat < -2.0 * M_PI)
		lat = remainder(lat, -2.0 * M_PI);

	if (lat > M_PI)
		lat = M_PI - lat;

	if (lat < -1.0 * M_PI)
		lat = -1.0 * M_PI - lat;

	if (lat > M_PI_2)
		lat = M_PI - lat;

	if (lat < -1.0 * M_PI_2)
		lat = -1.0 * M_PI - lat;

	return lat;
}

/**
 * Given two points on a unit sphere, calculate their distance apart in radians.
 */
double sphere_distance(const GEOGRAPHIC_POINT *s, const GEOGRAPHIC_POINT *e) {
	double d_lon = e->lon - s->lon;
	double cos_d_lon = cos(d_lon);
	double cos_lat_e = cos(e->lat);
	double sin_lat_e = sin(e->lat);
	double cos_lat_s = cos(s->lat);
	double sin_lat_s = sin(s->lat);

	double a1 = POW2(cos_lat_e * sin(d_lon));
	double a2 = POW2(cos_lat_s * sin_lat_e - sin_lat_s * cos_lat_e * cos_d_lon);
	double a = sqrt(a1 + a2);
	double b = sin_lat_s * sin_lat_e + cos_lat_s * cos_lat_e * cos_d_lon;
	return atan2(a, b);
}

/**
 * Given a starting location r, a distance and an azimuth
 * to the new point, compute the location of the projected point on the unit sphere.
 */
int sphere_project(const GEOGRAPHIC_POINT *r, double distance, double azimuth, GEOGRAPHIC_POINT *n) {
	double d = distance;
	double lat1 = r->lat;
	double lon1 = r->lon;
	double lat2, lon2;

	lat2 = asin(sin(lat1) * cos(d) + cos(lat1) * sin(d) * cos(azimuth));

	/* If we're going straight up or straight down, we don't need to calculate the longitude */
	/* TODO: this isn't quite true, what if we're going over the pole? */
	if (FP_EQUALS(azimuth, M_PI) || FP_EQUALS(azimuth, 0.0)) {
		lon2 = r->lon;
	} else {
		lon2 = lon1 + atan2(sin(azimuth) * sin(d) * cos(lat1), cos(d) - sin(lat1) * sin(lat2));
	}

	if (isnan(lat2) || isnan(lon2))
		return LW_FAILURE;

	n->lat = lat2;
	n->lon = lon2;

	return LW_SUCCESS;
}

double edge_distance_to_point(const GEOGRAPHIC_EDGE *e, const GEOGRAPHIC_POINT *gp, GEOGRAPHIC_POINT *closest) {
	double d1 = 1000000000.0, d2, d3, d_nearest;
	POINT3D n, p, k;
	GEOGRAPHIC_POINT gk, g_nearest;

	/* Zero length edge, */
	if (geographic_point_equals(&(e->start), &(e->end))) {
		*closest = e->start;
		return sphere_distance(&(e->start), gp);
	}

	robust_cross_product(&(e->start), &(e->end), &n);
	normalize(&n);
	geog2cart(gp, &p);
	vector_scale(&n, dot_product(&p, &n));
	vector_difference(&p, &n, &k);
	normalize(&k);
	cart2geog(&k, &gk);
	if (edge_contains_point(e, &gk)) {
		d1 = sphere_distance(gp, &gk);
	}
	d2 = sphere_distance(gp, &(e->start));
	d3 = sphere_distance(gp, &(e->end));

	d_nearest = d1;
	g_nearest = gk;

	if (d2 < d_nearest) {
		d_nearest = d2;
		g_nearest = e->start;
	}
	if (d3 < d_nearest) {
		d_nearest = d3;
		g_nearest = e->end;
	}
	if (closest)
		*closest = g_nearest;

	return d_nearest;
}

/**
 * Calculate the distance between two edges.
 * IMPORTANT: this test does not check for edge intersection!!! (distance == 0)
 * You have to check for intersection before calling this function.
 */
double edge_distance_to_edge(const GEOGRAPHIC_EDGE *e1, const GEOGRAPHIC_EDGE *e2, GEOGRAPHIC_POINT *closest1,
                             GEOGRAPHIC_POINT *closest2) {
	double d;
	GEOGRAPHIC_POINT gcp1s, gcp1e, gcp2s, gcp2e, c1, c2;
	double d1s = edge_distance_to_point(e1, &(e2->start), &gcp1s);
	double d1e = edge_distance_to_point(e1, &(e2->end), &gcp1e);
	double d2s = edge_distance_to_point(e2, &(e1->start), &gcp2s);
	double d2e = edge_distance_to_point(e2, &(e1->end), &gcp2e);

	d = d1s;
	c1 = gcp1s;
	c2 = e2->start;

	if (d1e < d) {
		d = d1e;
		c1 = gcp1e;
		c2 = e2->end;
	}

	if (d2s < d) {
		d = d2s;
		c1 = e1->start;
		c2 = gcp2s;
	}

	if (d2e < d) {
		d = d2e;
		c1 = e1->end;
		c2 = gcp2e;
	}

	if (closest1)
		*closest1 = c1;
	if (closest2)
		*closest2 = c2;

	return d;
}

int geographic_point_equals(const GEOGRAPHIC_POINT *g1, const GEOGRAPHIC_POINT *g2) {
	return FP_EQUALS(g1->lat, g2->lat) && FP_EQUALS(g1->lon, g2->lon);
}

/**
 * Returns non-zero if edges A and B interact. The type of interaction is given in the
 * return value with the bitmask elements defined above.
 */
uint32_t edge_intersects(const POINT3D *A1, const POINT3D *A2, const POINT3D *B1, const POINT3D *B2) {
	POINT3D AN, BN, VN; /* Normals to plane A and plane B */
	double ab_dot;
	int a1_side, a2_side, b1_side, b2_side;
	int rv = PIR_NO_INTERACT;

	/* Normals to the A-plane and B-plane */
	unit_normal(A1, A2, &AN);
	unit_normal(B1, B2, &BN);

	/* Are A-plane and B-plane basically the same? */
	ab_dot = dot_product(&AN, &BN);

	if (FP_EQUALS(fabs(ab_dot), 1.0)) {
		/* Co-linear case */
		if (point_in_cone(A1, A2, B1) || point_in_cone(A1, A2, B2) || point_in_cone(B1, B2, A1) ||
		    point_in_cone(B1, B2, A2)) {
			rv |= PIR_INTERSECTS;
			rv |= PIR_COLINEAR;
		}
		return rv;
	}

	/* What side of plane-A and plane-B do the end points */
	/* of A and B fall? */
	a1_side = dot_product_side(&BN, A1);
	a2_side = dot_product_side(&BN, A2);
	b1_side = dot_product_side(&AN, B1);
	b2_side = dot_product_side(&AN, B2);

	/* Both ends of A on the same side of plane B. */
	if (a1_side == a2_side && a1_side != 0) {
		/* No intersection. */
		return PIR_NO_INTERACT;
	}

	/* Both ends of B on the same side of plane A. */
	if (b1_side == b2_side && b1_side != 0) {
		/* No intersection. */
		return PIR_NO_INTERACT;
	}

	/* A straddles B and B straddles A, so... */
	if (a1_side != a2_side && (a1_side + a2_side) == 0 && b1_side != b2_side && (b1_side + b2_side) == 0) {
		/* Have to check if intersection point is inside both arcs */
		unit_normal(&AN, &BN, &VN);
		if (point_in_cone(A1, A2, &VN) && point_in_cone(B1, B2, &VN)) {
			return PIR_INTERSECTS;
		}

		/* Have to check if intersection point is inside both arcs */
		vector_scale(&VN, -1);
		if (point_in_cone(A1, A2, &VN) && point_in_cone(B1, B2, &VN)) {
			return PIR_INTERSECTS;
		}

		return PIR_NO_INTERACT;
	}

	/* The rest are all intersects variants... */
	rv |= PIR_INTERSECTS;

	/* A touches B */
	if (a1_side == 0) {
		/* Touches at A1, A2 is on what side? */
		rv |= (a2_side < 0 ? PIR_A_TOUCH_RIGHT : PIR_A_TOUCH_LEFT);
	} else if (a2_side == 0) {
		/* Touches at A2, A1 is on what side? */
		rv |= (a1_side < 0 ? PIR_A_TOUCH_RIGHT : PIR_A_TOUCH_LEFT);
	}

	/* B touches A */
	if (b1_side == 0) {
		/* Touches at B1, B2 is on what side? */
		rv |= (b2_side < 0 ? PIR_B_TOUCH_RIGHT : PIR_B_TOUCH_LEFT);
	} else if (b2_side == 0) {
		/* Touches at B2, B1 is on what side? */
		rv |= (b1_side < 0 ? PIR_B_TOUCH_RIGHT : PIR_B_TOUCH_LEFT);
	}

	return rv;
}

/**
 * Returns true if an intersection can be calculated, and places it in *g.
 * Returns false otherwise.
 */
int edge_intersection(const GEOGRAPHIC_EDGE *e1, const GEOGRAPHIC_EDGE *e2, GEOGRAPHIC_POINT *g) {
	POINT3D ea, eb, v;

	if (geographic_point_equals(&(e1->start), &(e2->start))) {
		*g = e1->start;
		return LW_TRUE;
	}
	if (geographic_point_equals(&(e1->end), &(e2->end))) {
		*g = e1->end;
		return LW_TRUE;
	}
	if (geographic_point_equals(&(e1->end), &(e2->start))) {
		*g = e1->end;
		return LW_TRUE;
	}
	if (geographic_point_equals(&(e1->start), &(e2->end))) {
		*g = e1->start;
		return LW_TRUE;
	}

	robust_cross_product(&(e1->start), &(e1->end), &ea);
	normalize(&ea);
	robust_cross_product(&(e2->start), &(e2->end), &eb);
	normalize(&eb);
	if (FP_EQUALS(fabs(dot_product(&ea, &eb)), 1.0)) {
		/* Parallel (maybe equal) edges! */
		/* Hack alert, only returning ONE end of the edge right now, most do better later. */
		/* Hack alert #2, returning a value of 2 to indicate a co-linear crossing event. */
		if (edge_contains_point(e1, &(e2->start))) {
			*g = e2->start;
			return 2;
		}
		if (edge_contains_point(e1, &(e2->end))) {
			*g = e2->end;
			return 2;
		}
		if (edge_contains_point(e2, &(e1->start))) {
			*g = e1->start;
			return 2;
		}
		if (edge_contains_point(e2, &(e1->end))) {
			*g = e1->end;
			return 2;
		}
	}
	unit_normal(&ea, &eb, &v);
	g->lat = atan2(v.z, sqrt(v.x * v.x + v.y * v.y));
	g->lon = atan2(v.y, v.x);
	if (edge_contains_point(e1, g) && edge_contains_point(e2, g)) {
		return LW_TRUE;
	} else {
		g->lat = -1.0 * g->lat;
		g->lon = g->lon + M_PI;
		if (g->lon > M_PI) {
			g->lon = -1.0 * (2.0 * M_PI - g->lon);
		}
		if (edge_contains_point(e1, g) && edge_contains_point(e2, g)) {
			return LW_TRUE;
		}
	}
	return LW_FALSE;
}

} // namespace duckdb
