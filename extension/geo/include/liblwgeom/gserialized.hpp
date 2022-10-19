#pragma once
#include "duckdb.hpp"
#include "liblwgeom/liblwgeom.hpp"

namespace duckdb {

/**
 * Check if a #GSERIALIZED has a bounding box without deserializing first.
 */
extern int gserialized_has_bbox(const GSERIALIZED *gser);

/**
 * Extract the SRID from the serialized form (it is packed into
 * three bytes so this is a handy function).
 */
extern int32_t gserialized_get_srid(const GSERIALIZED *g);

/**
 * Write the SRID into the serialized form (it is packed into
 * three bytes so this is a handy function).
 */
extern void gserialized_set_srid(GSERIALIZED *g, int32_t srid);

/**
 * Check if a #GSERIALIZED is empty without deserializing first.
 * Only checks if the number of elements of the parent geometry
 * is zero, will not catch collections of empty, eg:
 * GEOMETRYCOLLECTION(POINT EMPTY)
 */
extern int gserialized_is_empty(const GSERIALIZED *g);

/**
 * Allocate a new #GSERIALIZED from an #LWGEOM. For all non-point types, a bounding
 * box will be calculated and embedded in the serialization. The geodetic flag is used
 * to control the box calculation (cartesian or geocentric). If set, the size pointer
 * will contain the size of the final output, which is useful for setting the PgSQL
 * VARSIZE information.
 */
GSERIALIZED *gserialized_from_lwgeom(LWGEOM *geom, size_t *size);

/**
 * Allocate a new #LWGEOM from a #GSERIALIZED. The resulting #LWGEOM will have coordinates
 * that are double aligned and suitable for direct reading using getPoint2d_p_ro
 */
LWGEOM *lwgeom_from_gserialized(const GSERIALIZED *g);

/**
 * Extract the geometry type from the serialized form (it hides in
 * the anonymous data area, so this is a handy function).
 */
extern uint32_t gserialized_get_type(const GSERIALIZED *g);

/**
 * Pull the first point values of a #GSERIALIZED. Only works for POINTTYPE
 */
int gserialized_peek_first_point(const GSERIALIZED *g, POINT4D *out_point);

} // namespace duckdb
