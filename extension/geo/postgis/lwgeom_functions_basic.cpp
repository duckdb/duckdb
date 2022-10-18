#include "postgis/lwgeom_functions_basic.hpp"

#include "liblwgeom/gserialized.hpp"
#include "liblwgeom/liblwgeom.hpp"
#include "libpgcommon/lwgeom_pg.hpp"

#include <float.h>

namespace duckdb {

GSERIALIZED *LWGEOM_makepoint(double x, double y) {
	LWPOINT *point;
	GSERIALIZED *result;

	point = lwpoint_make2d(SRID_UNKNOWN, x, y);

	result = geometry_serialize((LWGEOM *)point);
	lwgeom_free((LWGEOM *)point);

	return result;
}

GSERIALIZED *LWGEOM_makepoint(double x, double y, double z) {
	LWPOINT *point;
	GSERIALIZED *result;

	point = lwpoint_make3dz(SRID_UNKNOWN, x, y, z);

	result = geometry_serialize((LWGEOM *)point);
	lwgeom_free((LWGEOM *)point);

	return result;
}

double ST_distance(GSERIALIZED *geom1, GSERIALIZED *geom2) {
	double mindist;
	LWGEOM *lwgeom1 = lwgeom_from_gserialized(geom1);
	LWGEOM *lwgeom2 = lwgeom_from_gserialized(geom2);
	gserialized_error_if_srid_mismatch(geom1, geom2, __func__);

	mindist = lwgeom_mindistance2d(lwgeom1, lwgeom2);

	lwgeom_free(lwgeom1);
	lwgeom_free(lwgeom2);

	/* if called with empty geometries the ingoing mindistance is untouched, and makes us return NULL*/
	if (mindist < FLT_MAX)
		return mindist;

	PG_ERROR_NULL();
	return mindist;
}

} // namespace duckdb
