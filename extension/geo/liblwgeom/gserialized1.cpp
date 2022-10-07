#include "liblwgeom/gserialized1.hpp"

#include <cassert>
#include <cstring>

namespace duckdb {

lwflags_t gserialized1_get_lwflags(const GSERIALIZED *g) {
	lwflags_t lwflags = 0;
	uint8_t gflags = g->gflags;
	FLAGS_SET_Z(lwflags, G1FLAGS_GET_Z(gflags));
	FLAGS_SET_M(lwflags, G1FLAGS_GET_M(gflags));
	FLAGS_SET_BBOX(lwflags, G1FLAGS_GET_BBOX(gflags));
	FLAGS_SET_GEODETIC(lwflags, G1FLAGS_GET_GEODETIC(gflags));
	FLAGS_SET_SOLID(lwflags, G1FLAGS_GET_SOLID(gflags));
	return lwflags;
}

/* handle missaligned uint32_t data */
static inline uint32_t gserialized1_get_uint32_t(const uint8_t *loc) {
	return *((uint32_t *)loc);
}

int gserialized1_has_bbox(const GSERIALIZED *gser) {
	return G1FLAGS_GET_BBOX(gser->gflags);
}

static size_t gserialized1_box_size(const GSERIALIZED *g) {
	if (G1FLAGS_GET_GEODETIC(g->gflags))
		return 6 * sizeof(float);
	else
		return 2 * G1FLAGS_NDIMS(g->gflags) * sizeof(float);
}

int gserialized1_read_gbox_p(const GSERIALIZED *g, GBOX *gbox) {

	/* Null input! */
	if (!(g && gbox))
		return LW_FAILURE;

	/* Initialize the flags on the box */
	gbox->flags = gserialized1_get_lwflags(g);

	/* Has pre-calculated box */
	if (G1FLAGS_GET_BBOX(g->gflags)) {
		int i = 0;
		float *fbox = (float *)(g->data);
		gbox->xmin = fbox[i++];
		gbox->xmax = fbox[i++];
		gbox->ymin = fbox[i++];
		gbox->ymax = fbox[i++];

		/* Geodetic? Read next dimension (geocentric Z) and return */
		if (G1FLAGS_GET_GEODETIC(g->gflags)) {
			gbox->zmin = fbox[i++];
			gbox->zmax = fbox[i++];
			return LW_SUCCESS;
		}
		/* Cartesian? Read extra dimensions (if there) and return */
		if (G1FLAGS_GET_Z(g->gflags)) {
			gbox->zmin = fbox[i++];
			gbox->zmax = fbox[i++];
		}
		if (G1FLAGS_GET_M(g->gflags)) {
			gbox->mmin = fbox[i++];
			gbox->mmax = fbox[i++];
		}
		return LW_SUCCESS;
	}
	return LW_FAILURE;
}

int32_t gserialized1_get_srid(const GSERIALIZED *s) {
	int32_t srid = 0;
	srid = srid | (s->srid[0] << 16);
	srid = srid | (s->srid[1] << 8);
	srid = srid | s->srid[2];
	/* Only the first 21 bits are set. Slide up and back to pull
	   the negative bits down, if we need them. */
	srid = (srid << 11) >> 11;

	/* 0 is our internal unknown value. We'll map back and forth here for now */
	if (srid == 0)
		return SRID_UNKNOWN;
	else
		return srid;
}

uint32_t gserialized1_get_type(const GSERIALIZED *g) {
	uint32_t *ptr;
	ptr = (uint32_t *)(g->data);
	if (G1FLAGS_GET_BBOX(g->gflags)) {
		ptr += (gserialized1_box_size(g) / sizeof(uint32_t));
	}
	return *ptr;
}

static LWPOINT *lwpoint_from_gserialized1_buffer(uint8_t *data_ptr, lwflags_t lwflags, size_t *size) {
	uint8_t *start_ptr = data_ptr;
	LWPOINT *point;
	uint32_t npoints = 0;

	assert(data_ptr);

	point = (LWPOINT *)lwalloc(sizeof(LWPOINT));
	point->srid = SRID_UNKNOWN; /* Default */
	point->bbox = NULL;
	point->type = POINTTYPE;
	point->flags = lwflags;

	data_ptr += 4;                                 /* Skip past the type. */
	npoints = gserialized1_get_uint32_t(data_ptr); /* Zero => empty geometry */
	data_ptr += 4;                                 /* Skip past the npoints. */

	if (npoints > 0)
		point->point = ptarray_construct_reference_data(FLAGS_GET_Z(lwflags), FLAGS_GET_M(lwflags), 1, data_ptr);
	else
		point->point = ptarray_construct(FLAGS_GET_Z(lwflags), FLAGS_GET_M(lwflags), 0); /* Empty point */

	data_ptr += npoints * FLAGS_NDIMS(lwflags) * sizeof(double);

	if (size)
		*size = data_ptr - start_ptr;

	return point;
}

LWGEOM *lwgeom_from_gserialized1_buffer(uint8_t *data_ptr, lwflags_t lwflags, size_t *g_size) {
	uint32_t type;

	assert(data_ptr);

	type = gserialized1_get_uint32_t(data_ptr);

	switch (type) {
	case POINTTYPE:
		return (LWGEOM *)lwpoint_from_gserialized1_buffer(data_ptr, lwflags, g_size);
		// Need to do with postgis

	default:
		// lwerror("Unknown geometry type: %d - %s", type, lwtype_name(type));
		return NULL;
	}
}

LWGEOM *lwgeom_from_gserialized1(const GSERIALIZED *g) {
	lwflags_t lwflags = 0;
	int32_t srid = 0;
	uint32_t lwtype = 0;
	uint8_t *data_ptr = NULL;
	LWGEOM *lwgeom = NULL;
	GBOX bbox;
	size_t size = 0;

	assert(g);

	srid = gserialized1_get_srid(g);
	lwtype = gserialized1_get_type(g);
	lwflags = gserialized1_get_lwflags(g);

	data_ptr = (uint8_t *)g->data;
	if (FLAGS_GET_BBOX(lwflags))
		data_ptr += gbox_serialized_size(lwflags);

	lwgeom = lwgeom_from_gserialized1_buffer(data_ptr, lwflags, &size);

	if (!lwgeom)
		// lwerror("%s: unable create geometry", __func__); /* Ooops! */
		return NULL;

	lwgeom->type = lwtype;
	lwgeom->flags = lwflags;

	if (gserialized1_read_gbox_p(g, &bbox) == LW_SUCCESS) {
		lwgeom->bbox = gbox_copy(&bbox);
	} else if (lwgeom_needs_bbox(lwgeom) && (lwgeom_calculate_gbox(lwgeom, &bbox) == LW_SUCCESS)) {
		lwgeom->bbox = gbox_copy(&bbox);
	} else {
		lwgeom->bbox = NULL;
	}

	lwgeom_set_srid(lwgeom, srid);

	return lwgeom;
}
} // namespace duckdb