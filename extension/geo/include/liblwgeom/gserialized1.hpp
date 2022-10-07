#pragma one

#include "liblwgeom/lwinline.hpp"

namespace duckdb {

/**
 * Macros for manipulating the 'flags' byte. A uint8_t used as follows:
 * VVSRGBMZ
 * Version bit, followed by
 * Validty, Solid, ReadOnly, Geodetic, HasBBox, HasM and HasZ flags.
 */
#define G1FLAG_Z        0x01
#define G1FLAG_M        0x02
#define G1FLAG_BBOX     0x04
#define G1FLAG_GEODETIC 0x08
#define G1FLAG_READONLY 0x10
#define G1FLAG_SOLID    0x20
/* VERSION BITS         0x40 */
/* VERSION BITS         0x80 */

#define G1FLAGS_GET_Z(gflags)        ((gflags)&G1FLAG_Z)
#define G1FLAGS_GET_M(gflags)        (((gflags)&G1FLAG_M) >> 1)
#define G1FLAGS_GET_BBOX(gflags)     (((gflags)&G1FLAG_BBOX) >> 2)
#define G1FLAGS_GET_GEODETIC(gflags) (((gflags)&G1FLAG_GEODETIC) >> 3)
#define G1FLAGS_GET_SOLID(gflags)    (((gflags)&G1FLAG_SOLID) >> 5)

#define G1FLAGS_SET_Z(gflags, value)    ((gflags) = (value) ? ((gflags) | G1FLAG_Z) : ((gflags) & ~G1FLAG_Z))
#define G1FLAGS_SET_M(gflags, value)    ((gflags) = (value) ? ((gflags) | G1FLAG_M) : ((gflags) & ~G1FLAG_M))
#define G1FLAGS_SET_BBOX(gflags, value) ((gflags) = (value) ? ((gflags) | G1FLAG_BBOX) : ((gflags) & ~G1FLAG_BBOX))
#define G1FLAGS_SET_GEODETIC(gflags, value)                                                                            \
	((gflags) = (value) ? ((gflags) | G1FLAG_GEODETIC) : ((gflags) & ~G1FLAG_GEODETIC))
#define G1FLAGS_SET_SOLID(gflags, value) ((gflags) = (value) ? ((gflags) | G1FLAG_SOLID) : ((gflags) & ~G1FLAG_SOLID))

#define G1FLAGS_NDIMS(gflags)     (2 + G1FLAGS_GET_Z(gflags) + G1FLAGS_GET_M(gflags))
#define G1FLAGS_GET_ZM(gflags)    (G1FLAGS_GET_M(gflags) + G1FLAGS_GET_Z(gflags) * 2)
#define G1FLAGS_NDIMS_BOX(gflags) (G1FLAGS_GET_GEODETIC(gflags) ? 3 : G1FLAGS_NDIMS(gflags))

/**
 * Read the flags from a #GSERIALIZED and return a standard lwflag
 * integer
 */
lwflags_t gserialized1_get_lwflags(const GSERIALIZED *g);

/**
 * Check if a #GSERIALIZED has a bounding box without deserializing first.
 */
int gserialized1_has_bbox(const GSERIALIZED *gser);

/**
 * Extract the SRID from the serialized form (it is packed into
 * three bytes so this is a handy function).
 */
int32_t gserialized1_get_srid(const GSERIALIZED *g);

/**
 * Extract the geometry type from the serialized form (it hides in
 * the anonymous data area, so this is a handy function).
 */
uint32_t gserialized1_get_type(const GSERIALIZED *g);

/**
 * Allocate a new #LWGEOM from a #GSERIALIZED. The resulting #LWGEOM will have coordinates
 * that are double aligned and suitable for direct reading using getPoint2d_p_ro
 */
LWGEOM *lwgeom_from_gserialized1(const GSERIALIZED *g);

} // namespace duckdb