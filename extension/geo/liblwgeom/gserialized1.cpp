#include "liblwgeom/gserialized1.hpp"

#include <cassert>
#include <cstring>

namespace duckdb {

static inline void gserialized1_copy_point(double *dptr, lwflags_t flags, POINT4D *out_point) {
	uint8_t dim = 0;
	out_point->x = dptr[dim++];
	out_point->y = dptr[dim++];

	if (G1FLAGS_GET_Z(flags)) {
		out_point->z = dptr[dim++];
	}
	if (G1FLAGS_GET_M(flags)) {
		out_point->m = dptr[dim];
	}
}

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

void gserialized1_set_srid(GSERIALIZED *s, int32_t srid) {
	srid = clamp_srid(srid);

	/* 0 is our internal unknown value.
	 * We'll map back and forth here for now */
	if (srid == SRID_UNKNOWN)
		srid = 0;

	s->srid[0] = (srid & 0x001F0000) >> 16;
	s->srid[1] = (srid & 0x0000FF00) >> 8;
	s->srid[2] = (srid & 0x000000FF);
}

static size_t gserialized1_is_empty_recurse(const uint8_t *p, int *isempty) {
	// int i;
	int32_t type, num;

	memcpy(&type, p, 4);
	memcpy(&num, p + 4, 4);

	// Need to do with postgis
	// if ( lwtype_is_collection(type) )
	// {
	// 	size_t lz = 8;
	// 	for ( i = 0; i < num; i++ )
	// 	{
	// 		lz += gserialized1_is_empty_recurse(p+lz, isempty);
	// 		if ( ! *isempty )
	// 			return lz;
	// 	}
	// 	*isempty = LW_TRUE;
	// 	return lz;
	// }
	// else
	{
		*isempty = (num == 0 ? LW_TRUE : LW_FALSE);
		return 8;
	}
}

int gserialized1_is_empty(const GSERIALIZED *g) {
	uint8_t *p = (uint8_t *)g;
	int isempty = 0;
	assert(g);

	p += 8; /* Skip varhdr and srid/flags */
	if (gserialized1_has_bbox(g))
		p += gserialized1_box_size(g); /* Skip the box */

	gserialized1_is_empty_recurse(p, &isempty);
	return isempty;
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

int gserialized1_peek_first_point(const GSERIALIZED *g, POINT4D *out_point) {
	uint8_t *geometry_start = ((uint8_t *)g->data);
	if (gserialized1_has_bbox(g)) {
		geometry_start += gserialized1_box_size(g);
	}

	uint32_t isEmpty = (((uint32_t *)geometry_start)[1]) == 0;
	if (isEmpty) {
		return LW_FAILURE;
	}

	uint32_t type = (((uint32_t *)geometry_start)[0]);
	/* Setup double_array_start depending on the geometry type */
	double *double_array_start = NULL;
	switch (type) {
	case (POINTTYPE):
		/* For points we only need to jump over the type and npoints 32b ints */
		double_array_start = (double *)(geometry_start + 2 * sizeof(uint32_t));
		break;

	default:
		// lwerror("%s is currently not implemented for type %d", __func__, type);
		return LW_FAILURE;
	}

	gserialized1_copy_point(double_array_start, g->gflags, out_point);
	return LW_SUCCESS;
}

} // namespace duckdb
