#include "liblwgeom/gserialized.hpp"

#include "liblwgeom/gserialized1.hpp"
#include "liblwgeom/gserialized2.hpp"
#include "libpgcommon/lwgeom_pg.hpp"

namespace duckdb {

/* First four bits don't change between v0 and v1 */
#define GFLAG_Z        0x01
#define GFLAG_M        0x02
#define GFLAG_BBOX     0x04
#define GFLAG_GEODETIC 0x08
/* v1 and v2 MUST share the same version bits */
#define GFLAG_VER_0                0x40
#define GFLAGS_GET_VERSION(gflags) (((gflags)&GFLAG_VER_0) >> 6)

/**
 * Check if a #GSERIALIZED has a bounding box without deserializing first.
 */
int gserialized_has_bbox(const GSERIALIZED *g) {
	if (GFLAGS_GET_VERSION(g->gflags))
		return gserialized2_has_bbox(g);
	else
		return gserialized1_has_bbox(g);
}

/**
 * Allocate a new #GSERIALIZED from an #LWGEOM. For all non-point types, a bounding
 * box will be calculated and embedded in the serialization. The geodetic flag is used
 * to control the box calculation (cartesian or geocentric). If set, the size pointer
 * will contain the size of the final output, which is useful for setting the PgSQL
 * VARSIZE information.
 */
GSERIALIZED *gserialized_from_lwgeom(LWGEOM *geom, size_t *size) {
	return gserialized2_from_lwgeom(geom, size);
}

/**
 * Allocate a new #LWGEOM from a #GSERIALIZED. The resulting #LWGEOM will have coordinates
 * that are double aligned and suitable for direct reading using getPoint2d_p_ro
 */
LWGEOM *lwgeom_from_gserialized(const GSERIALIZED *g) {
	if (GFLAGS_GET_VERSION(g->gflags))
		return lwgeom_from_gserialized2(g);
	else
		return lwgeom_from_gserialized1(g);
}

} // namespace duckdb
