#include "postgis/geography_centroid.hpp"

#include "liblwgeom/gserialized.hpp"
#include "liblwgeom/liblwgeom.hpp"
#include "liblwgeom/lwinline.hpp"

namespace duckdb {

GSERIALIZED *geography_centroid(GSERIALIZED *g, bool use_spheroid) {
	LWGEOM *lwgeom = NULL;
	LWGEOM *lwgeom_out = NULL;
	LWPOINT *lwpoint_out = NULL;
	GSERIALIZED *g_out = NULL;
	int32_t srid;
	SPHEROID s;

	/* Get our geometry object loaded into memory. */
	lwgeom = lwgeom_from_gserialized(g);

	if (g == NULL) {
		return nullptr;
	}

	srid = lwgeom_get_srid(lwgeom);

	/* on empty input, return empty output */
	// if (gserialized_is_empty(g)) {
	// 	LWCOLLECTION *empty = lwcollection_construct_empty(COLLECTIONTYPE, srid, 0, 0);
	// 	lwgeom_out = lwcollection_as_lwgeom(empty);
	// 	g_out = geography_serialize(lwgeom_out);
	// 	PG_RETURN_POINTER(g_out);
	// }

	/* Initialize spheroid */
	// spheroid_init_from_srid(fcinfo, srid, &s);

	/* Set to sphere if requested */
	if (!use_spheroid)
		s.a = s.b = s.radius;

	switch (lwgeom_get_type(lwgeom)) {

	case POINTTYPE: {
		/* centroid of a point is itself */
		return g;
	}

	default:
		// elog(ERROR, "ST_Centroid(geography) unhandled geography type");
		return nullptr;
	}

	// lwgeom_out = lwpoint_as_lwgeom(lwpoint_out);
	// g_out = geography_serialize(lwgeom_out);

	// PG_RETURN_POINTER(g_out);
	return nullptr;
}

} // namespace duckdb
