#include "liblwgeom/gserialized.hpp"
#include "libpgcommon/lwgeom_pg.hpp"

namespace duckdb {

// int
// GetLWPROJ(int32_t srid_from, int32_t srid_to, LWPROJ **pj)
// {
// 	/* get or initialize the cache for this round */
// 	PROJSRSCache* proj_cache = GetPROJSRSCache();
// 	if (!proj_cache)
// 		return LW_FAILURE;

// 	postgis_initialize_cache();
// 	/* Add the output srid to the cache if it's not already there */
// 	*pj = GetProjectionFromPROJCache(proj_cache, srid_from, srid_to);
// 	if (*pj == NULL)
// 	{
// 		*pj = AddToPROJSRSCache(proj_cache, srid_from, srid_to);
// 	}

// 	return pj != NULL;
// }

int spheroid_init_from_srid(int32_t srid, SPHEROID *s) {
	// 	LWPROJ *pj;
	// #if POSTGIS_PROJ_VERSION >= 48 && POSTGIS_PROJ_VERSION < 60
	// 	double major_axis, minor_axis, eccentricity_squared;
	// #endif

	// 	if ( GetLWPROJ(srid, srid, &pj) == LW_FAILURE)
	// 		return LW_FAILURE;

	// #if POSTGIS_PROJ_VERSION >= 60
	// 	if (!pj->source_is_latlong)
	// 		return LW_FAILURE;
	// 	spheroid_init(s, pj->source_semi_major_metre, pj->source_semi_minor_metre);

	// #elif POSTGIS_PROJ_VERSION >= 48
	// 	if (!pj_is_latlong(pj->pj_from))
	// 		return LW_FAILURE;
	// 	/* For newer versions of Proj we can pull the spheroid paramaeters and initialize */
	// 	/* using them */
	// 	pj_get_spheroid_defn(pj->pj_from, &major_axis, &eccentricity_squared);
	// 	minor_axis = major_axis * sqrt(1-eccentricity_squared);
	// 	spheroid_init(s, major_axis, minor_axis);

	// #else
	// 	if (!pj_is_latlong(pj->pj_from))
	// 		return LW_FAILURE;
	// 	/* For old versions of Proj we cannot lookup the spheroid parameters from the API */
	// 	/* So we use the WGS84 parameters (boo!) */
	// 	spheroid_init(s, WGS84_MAJOR_AXIS, WGS84_MINOR_AXIS);
	// #endif
	spheroid_init(s, WGS84_MAJOR_AXIS, WGS84_MINOR_AXIS);

	return LW_SUCCESS;
}

} // namespace duckdb
