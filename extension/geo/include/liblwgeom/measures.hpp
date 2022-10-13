#pragma once

#include "liblwgeom/liblwgeom_internal.hpp"

namespace duckdb {

/* for the measure functions*/
#define DIST_MAX -1
#define DIST_MIN 1

/**
 * Structure used in distance-calculations
 */
typedef struct {
	double distance; /*the distance between p1 and p2*/
	POINT2D p1;
	POINT2D p2;
	int mode;    /*the direction of looking, if thedir = -1 then we look for maxdistance and if it is 1 then we look for
	                mindistance*/
	int twisted; /*To preserve the order of incoming points to match the first and second point in shortest and longest
	                line*/
	double tolerance; /*the tolerance for dwithin and dfullywithin*/
} DISTPTS;

/*
 * Preprocessing functions
 */
int lw_dist2d_comp(const LWGEOM *lw1, const LWGEOM *lw2, DISTPTS *dl);
int lw_dist2d_distribute_bruteforce(const LWGEOM *lwg1, const LWGEOM *lwg2, DISTPTS *dl);
int lw_dist2d_recursive(const LWGEOM *lwg1, const LWGEOM *lwg2, DISTPTS *dl);

/*
 * Brute force functions
 */
int lw_dist2d_point_point(LWPOINT *point1, LWPOINT *point2, DISTPTS *dl);

} // namespace duckdb
