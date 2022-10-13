#pragma once

#include "liblwgeom/liblwgeom.hpp"

namespace duckdb {

/*
 * Proj4 caching has it's own mechanism, and is
 * stored globally as the cost of proj_create_crs_to_crs()
 * is so high (20-40ms) that the lifetime of fcinfo->flinfo->fn_extra
 * is too short to assist some work loads.
 */

// /* An entry in the PROJ SRS cache */
// typedef struct struct_PROJSRSCacheItem
// {
// 	int32_t srid_from;
// 	int32_t srid_to;
// 	uint64_t hits;
// 	LWPROJ *projection;
// }
// PROJSRSCacheItem;

// /* PROJ 4 lookup transaction cache methods */
// #define PROJ_CACHE_ITEMS 128

// /*
// * The proj4 cache holds a fixed number of reprojection
// * entries. In normal usage we don't expect it to have
// * many entries, so we always linearly scan the list.
// */
// typedef struct struct_PROJSRSCache
// {
// 	PROJSRSCacheItem PROJSRSCache[PROJ_CACHE_ITEMS];
// 	uint32_t PROJSRSCacheCount;
// 	MemoryContext PROJSRSCacheContext;
// }
// PROJSRSCache;

// int GetLWPROJ(int32_t srid_from, int32_t srid_to, LWPROJ **pj);
int spheroid_init_from_srid(int32_t srid, SPHEROID *s);

} // namespace duckdb
