#pragma once

#include "liblwgeom/liblwgeom.hpp"

namespace duckdb {

/**
 * Check if a #GSERIALIZED has a bounding box without deserializing first.
 */
extern int gserialized_has_bbox(const GSERIALIZED *gser);

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

} // namespace duckdb
