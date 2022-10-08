#include "liblwgeom/liblwgeom.hpp"
#include "liblwgeom/liblwgeom_internal.hpp"
#include "liblwgeom/lwinline.hpp"

#include <cassert>
#include <cstring>

namespace duckdb {

GBOX *gbox_new(lwflags_t flags) {
	GBOX *g = (GBOX *)lwalloc(sizeof(GBOX));
	gbox_init(g);
	g->flags = flags;
	return g;
}

void gbox_init(GBOX *gbox) {
	memset(gbox, 0, sizeof(GBOX));
}

int gbox_merge(const GBOX *new_box, GBOX *merge_box) {
	assert(merge_box);

	if (FLAGS_GET_ZM(merge_box->flags) != FLAGS_GET_ZM(new_box->flags))
		return LW_FAILURE;

	if (new_box->xmin < merge_box->xmin)
		merge_box->xmin = new_box->xmin;
	if (new_box->ymin < merge_box->ymin)
		merge_box->ymin = new_box->ymin;
	if (new_box->xmax > merge_box->xmax)
		merge_box->xmax = new_box->xmax;
	if (new_box->ymax > merge_box->ymax)
		merge_box->ymax = new_box->ymax;

	if (FLAGS_GET_Z(merge_box->flags) || FLAGS_GET_GEODETIC(merge_box->flags)) {
		if (new_box->zmin < merge_box->zmin)
			merge_box->zmin = new_box->zmin;
		if (new_box->zmax > merge_box->zmax)
			merge_box->zmax = new_box->zmax;
	}
	if (FLAGS_GET_M(merge_box->flags)) {
		if (new_box->mmin < merge_box->mmin)
			merge_box->mmin = new_box->mmin;
		if (new_box->mmax > merge_box->mmax)
			merge_box->mmax = new_box->mmax;
	}

	return LW_SUCCESS;
}

GBOX *gbox_copy(const GBOX *box) {
	GBOX *copy = (GBOX *)lwalloc(sizeof(GBOX));
	memcpy(copy, box, sizeof(GBOX));
	return copy;
}

int gbox_init_point3d(const POINT3D *p, GBOX *gbox) {
	gbox->xmin = gbox->xmax = p->x;
	gbox->ymin = gbox->ymax = p->y;
	gbox->zmin = gbox->zmax = p->z;
	return LW_SUCCESS;
}

int gbox_merge_point3d(const POINT3D *p, GBOX *gbox) {
	if (gbox->xmin > p->x)
		gbox->xmin = p->x;
	if (gbox->ymin > p->y)
		gbox->ymin = p->y;
	if (gbox->zmin > p->z)
		gbox->zmin = p->z;
	if (gbox->xmax < p->x)
		gbox->xmax = p->x;
	if (gbox->ymax < p->y)
		gbox->ymax = p->y;
	if (gbox->zmax < p->z)
		gbox->zmax = p->z;
	return LW_SUCCESS;
}

void gbox_duplicate(const GBOX *original, GBOX *duplicate) {
	assert(duplicate);
	assert(original);
	memcpy(duplicate, original, sizeof(GBOX));
}

size_t gbox_serialized_size(lwflags_t flags) {
	if (FLAGS_GET_GEODETIC(flags))
		return 6 * sizeof(float);
	else
		return 2 * FLAGS_NDIMS(flags) * sizeof(float);
}

static void ptarray_calculate_gbox_cartesian_2d(const POINTARRAY *pa, GBOX *gbox) {
	const POINT2D *p = getPoint2d_cp(pa, 0);

	gbox->xmax = gbox->xmin = p->x;
	gbox->ymax = gbox->ymin = p->y;

	for (uint32_t i = 1; i < pa->npoints; i++) {
		p = getPoint2d_cp(pa, i);
		gbox->xmin = FP_MIN(gbox->xmin, p->x);
		gbox->xmax = FP_MAX(gbox->xmax, p->x);
		gbox->ymin = FP_MIN(gbox->ymin, p->y);
		gbox->ymax = FP_MAX(gbox->ymax, p->y);
	}
}

int ptarray_calculate_gbox_cartesian(const POINTARRAY *pa, GBOX *gbox) {
	if (!pa || pa->npoints == 0)
		return LW_FAILURE;
	if (!gbox)
		return LW_FAILURE;

	int has_z = FLAGS_GET_Z(pa->flags);
	int has_m = FLAGS_GET_M(pa->flags);
	gbox->flags = lwflags(has_z, has_m, 0);
	int coordinates = 2 + has_z + has_m;

	switch (coordinates) {
	case 2: {
		ptarray_calculate_gbox_cartesian_2d(pa, gbox);
		break;
	}
		// case 3:
		// {
		// 	if (has_z)
		// 	{
		// 		ptarray_calculate_gbox_cartesian_3d(pa, gbox);
		// 	}
		// 	else
		// 	{
		// 		double zmin = gbox->zmin;
		// 		double zmax = gbox->zmax;
		// 		ptarray_calculate_gbox_cartesian_3d(pa, gbox);
		// 		gbox->mmin = gbox->zmin;
		// 		gbox->mmax = gbox->zmax;
		// 		gbox->zmin = zmin;
		// 		gbox->zmax = zmax;
		// 	}
		// 	break;
		// }
		// default:
		// {
		// 	ptarray_calculate_gbox_cartesian_4d(pa, gbox);
		// 	break;
		// }
	}
	return LW_SUCCESS;
}

static int lwpoint_calculate_gbox_cartesian(LWPOINT *point, GBOX *gbox) {
	if (!point)
		return LW_FAILURE;
	return ptarray_calculate_gbox_cartesian(point->point, gbox);
}

int lwgeom_calculate_gbox_cartesian(const LWGEOM *lwgeom, GBOX *gbox) {
	if (!lwgeom)
		return LW_FAILURE;

	switch (lwgeom->type) {
	case POINTTYPE:
		return lwpoint_calculate_gbox_cartesian((LWPOINT *)lwgeom, gbox);
		// Need to do with postgis
	}
	/* Never get here, please. */
	// lwerror("unsupported type (%d) - %s", lwgeom->type, lwtype_name(lwgeom->type));
	return LW_FAILURE;
}

} // namespace duckdb
