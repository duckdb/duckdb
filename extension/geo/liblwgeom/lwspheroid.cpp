#include "liblwgeom/liblwgeom.hpp"
#include "liblwgeom/lwgeodetic.hpp"

#include <math.h>

namespace duckdb {

/* In proj4.9, GeographicLib is in special header */
#ifdef PROJ_GEODESIC
#include <geodesic.h>
#include <proj.h>
#endif

/**
 * Initialize spheroid object based on major and minor axis
 */
void spheroid_init(SPHEROID *s, double a, double b) {
	s->a = a;
	s->b = b;
	s->f = (a - b) / a;
	s->e_sq = (a * a - b * b) / (a * a);
	s->radius = (2.0 * a + b) / 3.0;
}

#ifndef PROJ_GEODESIC

static double spheroid_mu2(double alpha, const SPHEROID *s) {
	double b2 = POW2(s->b);
	return POW2(cos(alpha)) * (POW2(s->a) - b2) / b2;
}

static double spheroid_big_a(double u2) {
	return 1.0 + (u2 / 16384.0) * (4096.0 + u2 * (-768.0 + u2 * (320.0 - 175.0 * u2)));
}

static double spheroid_big_b(double u2) {
	return (u2 / 1024.0) * (256.0 + u2 * (-128.0 + u2 * (74.0 - 47.0 * u2)));
}

#endif /* ! PROJ_GEODESIC */

#ifdef PROJ_GEODESIC

/**
 * Computes the shortest distance along the surface of the spheroid
 * between two points, using the inverse geodesic problem from
 * GeographicLib (Karney 2013).
 *
 * @param a - location of first point
 * @param b - location of second point
 * @param s - spheroid to calculate on
 * @return spheroidal distance between a and b in spheroid units
 */
double spheroid_distance(const GEOGRAPHIC_POINT *a, const GEOGRAPHIC_POINT *b, const SPHEROID *spheroid) {
	struct geod_geodesic gd;
	geod_init(&gd, spheroid->a, spheroid->f);
	double lat1 = a->lat * 180.0 / M_PI;
	double lon1 = a->lon * 180.0 / M_PI;
	double lat2 = b->lat * 180.0 / M_PI;
	double lon2 = b->lon * 180.0 / M_PI;
	double s12 = 0.0; /* return distance */
	geod_inverse(&gd, lat1, lon1, lat2, lon2, &s12, 0, 0);
	return s12;
}

/* Above use Proj GeographicLib */
#else /* ! PROJ_GEODESIC */
/* Below use pre-version 2.2 geodesic functions */

/**
 * Computes the shortest distance along the surface of the spheroid
 * between two points. Based on Vincenty's formula for the geodetic
 * inverse problem as described in "Geocentric Datum of Australia
 * Technical Manual", Chapter 4. Tested against:
 * http://mascot.gdbc.gov.bc.ca/mascot/util1a.html
 * and
 * http://www.ga.gov.au/nmd/geodesy/datums/vincenty_inverse.jsp
 *
 * @param a - location of first point.
 * @param b - location of second point.
 * @param s - spheroid to calculate on
 * @return spheroidal distance between a and b in spheroid units.
 */
double spheroid_distance(const GEOGRAPHIC_POINT *a, const GEOGRAPHIC_POINT *b, const SPHEROID *spheroid) {
	double lambda = (b->lon - a->lon);
	double f = spheroid->f;
	double omf = 1 - spheroid->f;
	double u1, u2;
	double cos_u1, cos_u2;
	double sin_u1, sin_u2;
	double big_a, big_b, delta_sigma;
	double alpha, sin_alpha, cos_alphasq, c;
	double sigma, sin_sigma, cos_sigma, cos2_sigma_m, sqrsin_sigma, last_lambda, omega;
	double cos_lambda, sin_lambda;
	double distance;
	int i = 0;

	/* Same point => zero distance */
	if (geographic_point_equals(a, b)) {
		return 0.0;
	}

	u1 = atan(omf * tan(a->lat));
	cos_u1 = cos(u1);
	sin_u1 = sin(u1);
	u2 = atan(omf * tan(b->lat));
	cos_u2 = cos(u2);
	sin_u2 = sin(u2);

	omega = lambda;
	do {
		cos_lambda = cos(lambda);
		sin_lambda = sin(lambda);
		sqrsin_sigma = POW2(cos_u2 * sin_lambda) + POW2((cos_u1 * sin_u2 - sin_u1 * cos_u2 * cos_lambda));
		sin_sigma = sqrt(sqrsin_sigma);
		cos_sigma = sin_u1 * sin_u2 + cos_u1 * cos_u2 * cos_lambda;
		sigma = atan2(sin_sigma, cos_sigma);
		sin_alpha = cos_u1 * cos_u2 * sin_lambda / sin(sigma);

		/* Numerical stability issue, ensure asin is not NaN */
		if (sin_alpha > 1.0)
			alpha = M_PI_2;
		else if (sin_alpha < -1.0)
			alpha = -1.0 * M_PI_2;
		else
			alpha = asin(sin_alpha);

		cos_alphasq = POW2(cos(alpha));
		cos2_sigma_m = cos(sigma) - (2.0 * sin_u1 * sin_u2 / cos_alphasq);

		/* Numerical stability issue, cos2 is in range */
		if (cos2_sigma_m > 1.0)
			cos2_sigma_m = 1.0;
		if (cos2_sigma_m < -1.0)
			cos2_sigma_m = -1.0;

		c = (f / 16.0) * cos_alphasq * (4.0 + f * (4.0 - 3.0 * cos_alphasq));
		last_lambda = lambda;
		lambda =
		    omega + (1.0 - c) * f * sin(alpha) *
		                (sigma + c * sin(sigma) * (cos2_sigma_m + c * cos(sigma) * (-1.0 + 2.0 * POW2(cos2_sigma_m))));
		i++;
	} while ((i < 999) && (lambda != 0.0) && (fabs((last_lambda - lambda) / lambda) > 1.0e-9));

	u2 = spheroid_mu2(alpha, spheroid);
	big_a = spheroid_big_a(u2);
	big_b = spheroid_big_b(u2);
	delta_sigma = big_b * sin_sigma *
	              (cos2_sigma_m + (big_b / 4.0) * (cos_sigma * (-1.0 + 2.0 * POW2(cos2_sigma_m)) -
	                                               (big_b / 6.0) * cos2_sigma_m * (-3.0 + 4.0 * sqrsin_sigma) *
	                                                   (-3.0 + 4.0 * POW2(cos2_sigma_m))));

	distance = spheroid->b * big_a * (sigma - delta_sigma);

	/* Algorithm failure, distance == NaN, fallback to sphere */
	if (distance != distance) {
		lwerror("spheroid_distance returned NaN: (%.20g %.20g) (%.20g %.20g) a = %.20g b = %.20g", a->lat, a->lon,
		        b->lat, b->lon, spheroid->a, spheroid->b);
		return spheroid->radius * sphere_distance(a, b);
	}

	return distance;
}

#endif /* else ! PROJ_GEODESIC */

} // namespace duckdb
