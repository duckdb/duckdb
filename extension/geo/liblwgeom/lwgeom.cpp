#include "liblwgeom/liblwgeom.hpp"
#include "liblwgeom/liblwgeom_internal.hpp"
#include "liblwgeom/lwinline.hpp"

#include <cassert>

namespace duckdb {

LWGEOM *lwpoint_as_lwgeom(const LWPOINT *obj) {
	if (obj == NULL)
		return NULL;
	return (LWGEOM *)obj;
}

void lwgeom_set_srid(LWGEOM *geom, int32_t srid) {
	// uint32_t i;

	geom->srid = srid;

	// Need to do with postgis
	// if ( lwgeom_is_collection(geom) )
	// {
	// 	/* All the children are set to the same SRID value */
	// 	LWCOLLECTION *col = lwgeom_as_lwcollection(geom);
	// 	for ( i = 0; i < col->ngeoms; i++ )
	// 	{
	// 		lwgeom_set_srid(col->geoms[i], srid);
	// 	}
	// }
}

int lwgeom_needs_bbox(const LWGEOM *geom) {
	assert(geom);
	if (geom->type == POINTTYPE) {
		return LW_FALSE;
	} else {
		return LW_TRUE;
	}
}

int lwgeom_has_srid(const LWGEOM *geom) {
	if (geom->srid != SRID_UNKNOWN)
		return LW_TRUE;

	return LW_FALSE;
}

/**
 * Ensure there's a box in the LWGEOM.
 * If the box is already there just return,
 * else compute it.
 */
void lwgeom_add_bbox(LWGEOM *lwgeom) {
	/* an empty LWGEOM has no bbox */
	if (lwgeom_is_empty(lwgeom))
		return;

	if (lwgeom->bbox)
		return;
	FLAGS_SET_BBOX(lwgeom->flags, 1);
	lwgeom->bbox = gbox_new(lwgeom->flags);
	lwgeom_calculate_gbox(lwgeom, lwgeom->bbox);
}

void lwgeom_free(LWGEOM *lwgeom) {
	/* There's nothing here to free... */
	if (!lwgeom)
		return;

	switch (lwgeom->type) {
	case POINTTYPE:
		lwpoint_free((LWPOINT *)lwgeom);
		break;

	default:
		// lwerror("lwgeom_free called with unknown type (%d) %s", lwgeom->type, lwtype_name(lwgeom->type));
		return;
	}
	return;
}

/**
 * Calculate the gbox for this geometry, a cartesian box or
 * geodetic box, depending on how it is flagged.
 */
int lwgeom_calculate_gbox(const LWGEOM *lwgeom, GBOX *gbox) {
	gbox->flags = lwgeom->flags;
	if (FLAGS_GET_GEODETIC(lwgeom->flags))
		return lwgeom_calculate_gbox_geodetic(lwgeom, gbox);
	else
		return lwgeom_calculate_gbox_cartesian(lwgeom, gbox);
}

} // namespace duckdb
