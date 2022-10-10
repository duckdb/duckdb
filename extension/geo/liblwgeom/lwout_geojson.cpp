#include "liblwgeom/liblwgeom_internal.hpp"
#include "liblwgeom/lwinline.hpp"

#include <cstring>

namespace duckdb {

static size_t pointArray_geojson_size(POINTARRAY *pa, int precision);

/**
 * Handle SRS
 */
static size_t asgeojson_srs_size(const char *srs) {
	int size;

	size = sizeof("'crs':{'type':'name',");
	size += sizeof("'properties':{'name':''}},");
	size += strlen(srs) * sizeof(char);

	return size;
}

static size_t asgeojson_srs_buf(char *output, const char *srs) {
	char *ptr = output;

	ptr += sprintf(ptr, "\"crs\":{\"type\":\"name\",");
	ptr += sprintf(ptr, "\"properties\":{\"name\":\"%s\"}},", srs);

	return (ptr - output);
}

/**
 * Handle Bbox
 */
static size_t asgeojson_bbox_size(int hasz, int precision) {
	int size;

	if (!hasz) {
		size = sizeof("\"bbox\":[,,,],");
		size += 2 * 2 * (OUT_MAX_BYTES_DOUBLE + precision);
	} else {
		size = sizeof("\"bbox\":[,,,,,],");
		size += 2 * 3 * (OUT_MAX_BYTES_DOUBLE + precision);
	}

	return size;
}

static size_t asgeojson_bbox_buf(char *output, GBOX *bbox, int hasz, int precision) {
	char *ptr = output;

	if (!hasz)
		ptr += sprintf(ptr, "\"bbox\":[%.*f,%.*f,%.*f,%.*f],", precision, bbox->xmin, precision, bbox->ymin, precision,
		               bbox->xmax, precision, bbox->ymax);
	else
		ptr += sprintf(ptr, "\"bbox\":[%.*f,%.*f,%.*f,%.*f,%.*f,%.*f],", precision, bbox->xmin, precision, bbox->ymin,
		               precision, bbox->zmin, precision, bbox->xmax, precision, bbox->ymax, precision, bbox->zmax);

	return (ptr - output);
}

static size_t pointArray_to_geojson(POINTARRAY *pa, char *output, int precision) {
	char *ptr = output;

	if (!FLAGS_GET_Z(pa->flags)) {
		for (uint32_t i = 0; i < pa->npoints; i++) {
			if (i) {
				*ptr = ',';
				ptr++;
			}
			const POINT2D *pt = getPoint2d_cp(pa, i);

			*ptr = '[';
			ptr++;
			ptr += lwprint_double(pt->x, precision, ptr);
			*ptr = ',';
			ptr++;
			ptr += lwprint_double(pt->y, precision, ptr);
			*ptr = ']';
			ptr++;
		}
	} else {
		for (uint32_t i = 0; i < pa->npoints; i++) {
			if (i) {
				*ptr = ',';
				ptr++;
			}

			const POINT3D *pt = getPoint3d_cp(pa, i);
			*ptr = '[';
			ptr++;
			ptr += lwprint_double(pt->x, precision, ptr);
			*ptr = ',';
			ptr++;
			ptr += lwprint_double(pt->y, precision, ptr);
			*ptr = ',';
			ptr++;
			ptr += lwprint_double(pt->z, precision, ptr);
			*ptr = ']';
			ptr++;
		}
	}
	*ptr = '\0';

	return (ptr - output);
}

/**
 * Point Geometry
 */

static size_t asgeojson_point_size(const LWPOINT *point, const char *srs, GBOX *bbox, int precision) {
	int size;

	size = pointArray_geojson_size(point->point, precision);
	size += sizeof("{'type':'Point',");
	size += sizeof("'coordinates':}");

	if (lwpoint_is_empty(point))
		size += 2; /* [] */

	if (srs)
		size += asgeojson_srs_size(srs);
	if (bbox)
		size += asgeojson_bbox_size(FLAGS_GET_Z(point->flags), precision);

	return size;
}

static size_t asgeojson_point_buf(const LWPOINT *point, const char *srs, char *output, GBOX *bbox, int precision) {
	char *ptr = output;

	ptr += sprintf(ptr, "{\"type\":\"Point\",");
	if (srs)
		ptr += asgeojson_srs_buf(ptr, srs);
	if (bbox)
		ptr += asgeojson_bbox_buf(ptr, bbox, FLAGS_GET_Z(point->flags), precision);

	ptr += sprintf(ptr, "\"coordinates\":");
	if (lwpoint_is_empty(point))
		ptr += sprintf(ptr, "[]");
	ptr += pointArray_to_geojson(point->point, ptr, precision);
	ptr += sprintf(ptr, "}");

	return (ptr - output);
}

static lwvarlena_t *asgeojson_point(const LWPOINT *point, const char *srs, GBOX *bbox, int precision) {
	uint32_t size = asgeojson_point_size(point, srs, bbox, precision);
	lwvarlena_t *output = (lwvarlena_t *)lwalloc(size + LWVARHDRSZ);
	size = asgeojson_point_buf(point, srs, output->data, bbox, precision);
	LWSIZE_SET(output->size, size + LWVARHDRSZ);
	return output;
}

/**
 * Takes a GEOMETRY and returns a GeoJson representation
 */
lwvarlena_t *lwgeom_to_geojson(const LWGEOM *geom, const char *srs, int precision, int has_bbox) {
	int type = geom->type;
	GBOX *bbox = NULL;
	GBOX tmp = {0};

	if (has_bbox) {
		/* Whether these are geography or geometry,
		   the GeoJSON expects a cartesian bounding box */
		lwgeom_calculate_gbox_cartesian(geom, &tmp);
		bbox = &tmp;
	}

	switch (type) {
	case POINTTYPE:
		return asgeojson_point((LWPOINT *)geom, srs, bbox, precision);
		// Need to do with postgis

	default:
		// lwerror("lwgeom_to_geojson: '%s' geometry type not supported",
		//         lwtype_name(type));
		return NULL;
	}

	/* Never get here */
	return NULL;
}

/**
 * Returns maximum size of rendered pointarray in bytes.
 */
static size_t pointArray_geojson_size(POINTARRAY *pa, int precision) {
	if (FLAGS_NDIMS(pa->flags) == 2)
		return (OUT_MAX_BYTES_DOUBLE + precision + sizeof(",")) * 2 * pa->npoints + sizeof(",[]");

	return (OUT_MAX_BYTES_DOUBLE + precision + sizeof(",,")) * 3 * pa->npoints + sizeof(",[]");
}

} // namespace duckdb
