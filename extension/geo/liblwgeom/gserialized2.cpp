#include "liblwgeom/gserialized2.hpp"

#include <cassert>
#include <cstring>

namespace duckdb {

static size_t gserialized2_from_lwgeom_any(const LWGEOM *geom, uint8_t *buf);

/* handle missaligned uint32_t data */
static inline uint32_t gserialized2_get_uint32_t(const uint8_t *loc) {
	return *((uint32_t *)loc);
}

lwflags_t gserialized2_get_lwflags(const GSERIALIZED *g) {
	lwflags_t lwflags = 0;
	uint8_t gflags = g->gflags;
	FLAGS_SET_Z(lwflags, G2FLAGS_GET_Z(gflags));
	FLAGS_SET_M(lwflags, G2FLAGS_GET_M(gflags));
	FLAGS_SET_BBOX(lwflags, G2FLAGS_GET_BBOX(gflags));
	FLAGS_SET_GEODETIC(lwflags, G2FLAGS_GET_GEODETIC(gflags));
	if (G2FLAGS_GET_EXTENDED(gflags)) {
		uint64_t xflags = 0;
		memcpy(&xflags, g->data, sizeof(uint64_t));
		FLAGS_SET_SOLID(lwflags, xflags & G2FLAG_X_SOLID);
	}
	return lwflags;
}

static int lwflags_uses_extended_flags(lwflags_t lwflags) {
	lwflags_t core_lwflags = LWFLAG_Z | LWFLAG_M | LWFLAG_BBOX | LWFLAG_GEODETIC;
	return (lwflags & (~core_lwflags)) != 0;
}

uint8_t lwflags_get_g2flags(lwflags_t lwflags) {
	uint8_t gflags = 0;
	G2FLAGS_SET_Z(gflags, FLAGS_GET_Z(lwflags));
	G2FLAGS_SET_M(gflags, FLAGS_GET_M(lwflags));
	G2FLAGS_SET_BBOX(gflags, FLAGS_GET_BBOX(lwflags));
	G2FLAGS_SET_GEODETIC(gflags, FLAGS_GET_GEODETIC(lwflags));
	G2FLAGS_SET_EXTENDED(gflags, lwflags_uses_extended_flags(lwflags));
	G2FLAGS_SET_VERSION(gflags, 1);
	return gflags;
}

static inline size_t gserialized2_box_size(const GSERIALIZED *g) {
	if (G2FLAGS_GET_GEODETIC(g->gflags))
		return 6 * sizeof(float);
	else
		return 2 * G2FLAGS_NDIMS(g->gflags) * sizeof(float);
}

/* Returns a pointer to the start of the geometry data */
static inline uint8_t *gserialized2_get_geometry_p(const GSERIALIZED *g) {
	uint32_t extra_data_bytes = 0;
	if (gserialized2_has_extended(g))
		extra_data_bytes += sizeof(uint64_t);

	if (gserialized2_has_bbox(g))
		extra_data_bytes += gserialized2_box_size(g);

	return ((uint8_t *)g->data) + extra_data_bytes;
}

uint32_t gserialized2_get_type(const GSERIALIZED *g) {
	uint8_t *ptr = gserialized2_get_geometry_p(g);
	return *((uint32_t *)(ptr));
}

int32_t gserialized2_get_srid(const GSERIALIZED *g) {
	int32_t srid = 0;
	srid = srid | (g->srid[0] << 16);
	srid = srid | (g->srid[1] << 8);
	srid = srid | (g->srid[2]);
	/* Only the first 21 bits are set. Slide up and back to pull
	   the negative bits down, if we need them. */
	srid = (srid << 11) >> 11;

	/* 0 is our internal unknown value. We'll map back and forth here for now */
	if (srid == 0)
		return SRID_UNKNOWN;
	else
		return srid;
}

static size_t gserialized2_from_extended_flags(lwflags_t lwflags, uint8_t *buf) {
	if (lwflags_uses_extended_flags(lwflags)) {
		uint64_t xflags = 0;
		if (FLAGS_GET_SOLID(lwflags))
			xflags |= G2FLAG_X_SOLID;

		// G2FLAG_X_CHECKED_VALID
		// G2FLAG_X_IS_VALID
		// G2FLAG_X_HAS_HASH

		memcpy(buf, &xflags, sizeof(uint64_t));
		return sizeof(uint64_t);
	}
	return 0;
}

void gserialized2_set_srid(GSERIALIZED *g, int32_t srid) {
	srid = clamp_srid(srid);

	/* 0 is our internal unknown value.
	 * We'll map back and forth here for now */
	if (srid == SRID_UNKNOWN)
		srid = 0;

	g->srid[0] = (srid & 0x001F0000) >> 16;
	g->srid[1] = (srid & 0x0000FF00) >> 8;
	g->srid[2] = (srid & 0x000000FF);
}

static size_t gserialized2_from_gbox(const GBOX *gbox, uint8_t *buf) {
	uint8_t *loc = buf;
	float *f;
	uint8_t i = 0;
	size_t return_size;

	assert(buf);

	f = (float *)buf;
	f[i++] = next_float_down(gbox->xmin);
	f[i++] = next_float_up(gbox->xmax);
	f[i++] = next_float_down(gbox->ymin);
	f[i++] = next_float_up(gbox->ymax);
	loc += 4 * sizeof(float);

	if (FLAGS_GET_GEODETIC(gbox->flags)) {
		f[i++] = next_float_down(gbox->zmin);
		f[i++] = next_float_up(gbox->zmax);
		loc += 2 * sizeof(float);

		return_size = (size_t)(loc - buf);
		return return_size;
	}

	if (FLAGS_GET_Z(gbox->flags)) {
		f[i++] = next_float_down(gbox->zmin);
		f[i++] = next_float_up(gbox->zmax);
		loc += 2 * sizeof(float);
	}

	if (FLAGS_GET_M(gbox->flags)) {
		f[i++] = next_float_down(gbox->mmin);
		f[i++] = next_float_up(gbox->mmax);
		loc += 2 * sizeof(float);
	}
	return_size = (size_t)(loc - buf);
	return return_size;
}

int gserialized2_has_bbox(const GSERIALIZED *g) {
	return G2FLAGS_GET_BBOX(g->gflags);
}

int gserialized2_has_extended(const GSERIALIZED *g) {
	return G2FLAGS_GET_EXTENDED(g->gflags);
}

/* Public function */
GSERIALIZED *gserialized2_from_lwgeom(LWGEOM *geom, size_t *size) {
	size_t expected_size = 0;
	size_t return_size = 0;
	uint8_t *ptr = NULL;
	GSERIALIZED *g = NULL;
	assert(geom);

	/*
	** See if we need a bounding box, add one if we don't have one.
	*/
	if ((!geom->bbox) && lwgeom_needs_bbox(geom) && (!lwgeom_is_empty(geom))) {
		lwgeom_add_bbox(geom);
	}

	/*
	** Harmonize the flags to the state of the lwgeom
	*/
	FLAGS_SET_BBOX(geom->flags, (geom->bbox ? 1 : 0));

	/* Set up the uint8_t buffer into which we are going to write the serialized geometry. */
	expected_size = gserialized2_from_lwgeom_size(geom);
	ptr = (uint8_t *)lwalloc(expected_size);
	g = (GSERIALIZED *)(ptr);

	/* Set the SRID! */
	gserialized2_set_srid(g, geom->srid);
	/*
	** We are aping PgSQL code here, PostGIS code should use
	** VARSIZE to set this for real.
	*/
	LWSIZE_SET(g->size, expected_size);
	g->gflags = lwflags_get_g2flags(geom->flags);

	/* Move write head past size, srid and flags. */
	ptr += 8;

	/* Write in the extended flags if necessary */
	ptr += gserialized2_from_extended_flags(geom->flags, ptr);

	/* Write in the serialized form of the gbox, if necessary. */
	if (geom->bbox)
		ptr += gserialized2_from_gbox(geom->bbox, ptr);

	/* Write in the serialized form of the geometry. */
	ptr += gserialized2_from_lwgeom_any(geom, ptr);

	/* Calculate size as returned by data processing functions. */
	return_size = ptr - (uint8_t *)g;

	assert(expected_size == return_size);
	if (size) /* Return the output size to the caller if necessary. */
		*size = return_size;

	return g;
}

int gserialized2_read_gbox_p(const GSERIALIZED *g, GBOX *gbox) {
	uint8_t gflags = g->gflags;
	/* Null input! */
	if (!(g && gbox))
		return LW_FAILURE;

	/* Initialize the flags on the box */
	gbox->flags = gserialized2_get_lwflags(g);

	/* Has pre-calculated box */
	if (G2FLAGS_GET_BBOX(gflags)) {
		int i = 0;
		const float *fbox = gserialized2_get_float_box_p(g, NULL);
		gbox->xmin = fbox[i++];
		gbox->xmax = fbox[i++];
		gbox->ymin = fbox[i++];
		gbox->ymax = fbox[i++];

		/* Geodetic? Read next dimension (geocentric Z) and return */
		if (G2FLAGS_GET_GEODETIC(gflags)) {
			gbox->zmin = fbox[i++];
			gbox->zmax = fbox[i++];
			return LW_SUCCESS;
		}
		/* Cartesian? Read extra dimensions (if there) and return */
		if (G2FLAGS_GET_Z(gflags)) {
			gbox->zmin = fbox[i++];
			gbox->zmax = fbox[i++];
		}
		if (G2FLAGS_GET_M(gflags)) {
			gbox->mmin = fbox[i++];
			gbox->mmax = fbox[i++];
		}
		return LW_SUCCESS;
	}
	return LW_FAILURE;
}

static LWPOINT *lwpoint_from_gserialized2_buffer(uint8_t *data_ptr, lwflags_t lwflags, size_t *size, int32_t srid) {
	uint8_t *start_ptr = data_ptr;
	LWPOINT *point;
	uint32_t npoints = 0;

	assert(data_ptr);

	point = (LWPOINT *)lwalloc(sizeof(LWPOINT));
	point->srid = srid;
	point->bbox = NULL;
	point->type = POINTTYPE;
	point->flags = lwflags;

	data_ptr += 4;                                 /* Skip past the type. */
	npoints = gserialized2_get_uint32_t(data_ptr); /* Zero => empty geometry */
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

LWGEOM *lwgeom_from_gserialized2_buffer(uint8_t *data_ptr, lwflags_t lwflags, size_t *g_size, int32_t srid) {
	uint32_t type;

	assert(data_ptr);

	type = gserialized2_get_uint32_t(data_ptr);

	switch (type) {
	case POINTTYPE:
		return (LWGEOM *)lwpoint_from_gserialized2_buffer(data_ptr, lwflags, g_size, srid);
		// Need to do with postgis

	default:
		// lwerror("Unknown geometry type: %d - %s", type, lwtype_name(type));
		return NULL;
	}
}

static size_t gserialized2_from_lwpoint_size(const LWPOINT *point) {
	size_t size = 4; /* Type number. */

	assert(point);

	size += 4; /* Number of points (one or zero (empty)). */
	size += point->point->npoints * FLAGS_NDIMS(point->flags) * sizeof(double);

	return size;
}

static size_t gserialized2_from_any_size(const LWGEOM *geom) {
	switch (geom->type) {
	case POINTTYPE:
		return gserialized2_from_lwpoint_size((LWPOINT *)geom);
	// Need to do with postgis
	default:
		// lwerror("Unknown geometry type: %d - %s", geom->type, lwtype_name(geom->type));
		return 0;
	}
}

/* Public function */
size_t gserialized2_from_lwgeom_size(const LWGEOM *geom) {
	size_t size = 8; /* Header overhead (varsize+flags+srid) */
	assert(geom);

	/* Reserve space for extended flags */
	if (lwflags_uses_extended_flags(geom->flags))
		size += 8;

	/* Reserve space for bounding box */
	if (geom->bbox)
		size += gbox_serialized_size(geom->flags);

	size += gserialized2_from_any_size(geom);

	return size;
}

static size_t gserialized2_from_lwpoint(const LWPOINT *point, uint8_t *buf) {
	uint8_t *loc;
	int ptsize = ptarray_point_size(point->point);
	int type = POINTTYPE;

	assert(point);
	assert(buf);

	if (FLAGS_GET_ZM(point->flags) != FLAGS_GET_ZM(point->point->flags))
		// lwerror("Dimensions mismatch in lwpoint");
		return 0;

	loc = buf;

	/* Write in the type. */
	memcpy(loc, &type, sizeof(uint32_t));
	loc += sizeof(uint32_t);
	/* Write in the number of points (0 => empty). */
	memcpy(loc, &(point->point->npoints), sizeof(uint32_t));
	loc += sizeof(uint32_t);

	/* Copy in the ordinates. */
	if (point->point->npoints > 0) {
		memcpy(loc, getPoint_internal(point->point, 0), ptsize);
		loc += ptsize;
	}

	return (size_t)(loc - buf);
}

LWGEOM *lwgeom_from_gserialized2(const GSERIALIZED *g) {
	lwflags_t lwflags = 0;
	int32_t srid = 0;
	uint32_t lwtype = 0;
	uint8_t *data_ptr = NULL;
	LWGEOM *lwgeom = NULL;
	GBOX bbox;
	size_t size = 0;

	assert(g);

	srid = gserialized2_get_srid(g);
	lwtype = gserialized2_get_type(g);
	lwflags = gserialized2_get_lwflags(g);

	data_ptr = (uint8_t *)g->data;

	/* Skip optional flags */
	if (G2FLAGS_GET_EXTENDED(g->gflags)) {
		data_ptr += sizeof(uint64_t);
	}

	/* Skip over optional bounding box */
	if (FLAGS_GET_BBOX(lwflags))
		data_ptr += gbox_serialized_size(lwflags);

	lwgeom = lwgeom_from_gserialized2_buffer(data_ptr, lwflags, &size, srid);

	if (!lwgeom)
		// lwerror("%s: unable create geometry", __func__); /* Ooops! */
		return NULL;

	lwgeom->type = lwtype;
	lwgeom->flags = lwflags;

	if (gserialized2_read_gbox_p(g, &bbox) == LW_SUCCESS) {
		lwgeom->bbox = gbox_copy(&bbox);
	} else if (lwgeom_needs_bbox(lwgeom) && (lwgeom_calculate_gbox(lwgeom, &bbox) == LW_SUCCESS)) {
		lwgeom->bbox = gbox_copy(&bbox);
	} else {
		lwgeom->bbox = NULL;
	}

	return lwgeom;
}

static size_t gserialized2_from_lwgeom_any(const LWGEOM *geom, uint8_t *buf) {
	assert(geom);
	assert(buf);

	switch (geom->type) {
	case POINTTYPE:
		return gserialized2_from_lwpoint((LWPOINT *)geom, buf);
	// Need to do with postgis
	default:
		// lwerror("Unknown geometry type: %d - %s", geom->type, lwtype_name(geom->type));
		return 0;
	}
}

const float *gserialized2_get_float_box_p(const GSERIALIZED *g, size_t *ndims) {
	uint8_t *ptr = (uint8_t *)(g->data);
	size_t bndims = G2FLAGS_NDIMS_BOX(g->gflags);

	if (ndims)
		*ndims = bndims;

	/* Cannot do anything if there's no box */
	if (!(g && gserialized_has_bbox(g)))
		return NULL;

	/* Advance past optional extended flags */
	if (gserialized2_has_extended(g))
		ptr += 8;

	return (const float *)(ptr);
}

} // namespace duckdb