#pragma once
#include "liblwgeom/liblwgeom.hpp"

namespace duckdb {

/*
 * Size of point represeneted in the POINTARRAY
 * 16 for 2d, 24 for 3d, 32 for 4d
 */
static inline size_t ptarray_point_size(const POINTARRAY *pa) {
	return sizeof(double) * FLAGS_NDIMS(pa->flags);
}

/*
 * Get a pointer to Nth point of a POINTARRAY
 * You'll need to cast it to appropriate dimensioned point.
 * Note that if you cast to a higher dimensional point you'll
 * possibly corrupt the POINTARRAY.
 *
 * Casting to returned pointer to POINT2D* should be safe,
 * as gserialized format always keeps the POINTARRAY pointer
 * aligned to double boundary.
 *
 * WARNING: Don't cast this to a POINT!
 * it would not be reliable due to memory alignment constraints
 */
static inline uint8_t *getPoint_internal(const POINTARRAY *pa, uint32_t n) {
	size_t size;
	uint8_t *ptr;

#if PARANOIA_LEVEL > 0
	assert(pa);
	assert(n <= pa->npoints);
	assert(n <= pa->maxpoints);
#endif

	size = ptarray_point_size(pa);
	ptr = pa->serialized_pointlist + size * n;

	return ptr;
}

/**
 * Returns a POINT2D pointer into the POINTARRAY serialized_ptlist,
 * suitable for reading from. This is very high performance
 * and declared const because you aren't allowed to muck with the
 * values, only read them.
 */
static inline const POINT2D *getPoint2d_cp(const POINTARRAY *pa, uint32_t n) {
	return (const POINT2D *)getPoint_internal(pa, n);
}

static inline int lwpoint_is_empty(const LWPOINT *point) {
	return !point->point || point->point->npoints < 1;
}

/**
 * Return true or false depending on whether a geometry is an "empty"
 * geometry (no vertices members)
 */
static inline int lwgeom_is_empty(const LWGEOM *geom) {
	switch (geom->type) {
	case POINTTYPE:
		return lwpoint_is_empty((LWPOINT *)geom);
		break;
	// Need to do with postgis
	default:
		return LW_FALSE;
		break;
	}
}

} // namespace duckdb
