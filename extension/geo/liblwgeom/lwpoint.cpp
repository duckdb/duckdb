#include "liblwgeom/liblwgeom.hpp"
#include "liblwgeom/liblwgeom_internal.hpp"
#include "liblwgeom/lwinline.hpp"

namespace duckdb {

LWPOINT *lwpoint_construct_empty(int32_t srid, char hasz, char hasm) {
	LWPOINT *result = (LWPOINT *)lwalloc(sizeof(LWPOINT));
	result->type = POINTTYPE;
	result->flags = lwflags(hasz, hasm, 0);
	result->srid = srid;
	result->point = ptarray_construct(hasz, hasm, 0);
	result->bbox = NULL;
	return result;
}

/*
 * Construct a new point.  point will not be copied
 * use SRID=SRID_UNKNOWN for unknown SRID (will have 8bit type's S = 0)
 */
LWPOINT *lwpoint_construct(int32_t srid, GBOX *bbox, POINTARRAY *point) {
	LWPOINT *result;
	lwflags_t flags = 0;

	if (point == NULL)
		return NULL; /* error */

	result = (LWPOINT *)lwalloc(sizeof(LWPOINT));
	result->type = POINTTYPE;
	FLAGS_SET_Z(flags, FLAGS_GET_Z(point->flags));
	FLAGS_SET_M(flags, FLAGS_GET_M(point->flags));
	FLAGS_SET_BBOX(flags, bbox ? 1 : 0);
	result->flags = flags;
	result->srid = srid;
	result->point = point;
	result->bbox = bbox;

	return result;
}

void lwpoint_free(LWPOINT *pt) {
	if (!pt)
		return;

	if (pt->bbox)
		lwfree(pt->bbox);
	if (pt->point)
		ptarray_free(pt->point);
	lwfree(pt);
}

} // namespace duckdb