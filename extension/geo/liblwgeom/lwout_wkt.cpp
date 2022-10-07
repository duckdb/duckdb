#include "liblwgeom/lwgeom_wkt.hpp"
#include "liblwgeom/lwinline.hpp"
#include "liblwgeom/stringbuffer.hpp"

namespace duckdb {

/*
 * ISO format uses both Z and M qualifiers.
 * Extended format only uses an M qualifier for 3DM variants, where it is not
 * clear what the third dimension represents.
 * SFSQL format never has more than two dimensions, so no qualifiers.
 */
static void dimension_qualifiers_to_wkt_sb(const LWGEOM *geom, stringbuffer_t *sb, uint8_t variant) {

	/* Extended WKT: POINTM(0 0 0) */
#if 0
	if ( (variant & WKT_EXTENDED) && ! (variant & WKT_IS_CHILD) && FLAGS_GET_M(geom->flags) && (!FLAGS_GET_Z(geom->flags)) )
#else
	if ((variant & WKT_EXTENDED) && FLAGS_GET_M(geom->flags) && (!FLAGS_GET_Z(geom->flags)))
#endif
	{
		stringbuffer_append_len(sb, "M", 1); /* "M" */
		return;
	}

	/* ISO WKT: POINT ZM (0 0 0 0) */
	if ((variant & WKT_ISO) && (FLAGS_NDIMS(geom->flags) > 2)) {
		stringbuffer_append_len(sb, " ", 1);
		if (FLAGS_GET_Z(geom->flags))
			stringbuffer_append_len(sb, "Z", 1);
		if (FLAGS_GET_M(geom->flags))
			stringbuffer_append_len(sb, "M", 1);
		stringbuffer_append_len(sb, " ", 1);
	}
}

/*
 * Write an empty token out, padding with a space if
 * necessary.
 */
static void empty_to_wkt_sb(stringbuffer_t *sb) {
	if (!strchr(" ,(", stringbuffer_lastchar(sb))) /* "EMPTY" */
	{
		stringbuffer_append_len(sb, " ", 1);
	}
	stringbuffer_append_len(sb, "EMPTY", 5);
}

inline static void coordinate_to_wkt_sb(double *coords, stringbuffer_t *sb, uint32_t dimensions, int precision) {
	uint32_t d = 0;
	stringbuffer_append_double(sb, coords[d], precision);

	for (d = 1; d < dimensions; d++) {
		stringbuffer_append_len(sb, " ", 1);
		stringbuffer_append_double(sb, coords[d], precision);
	}
}

/*
 * Point array is a list of coordinates. Depending on output mode,
 * we may suppress some dimensions. ISO and Extended formats include
 * all dimensions. Standard OGC output only includes X/Y coordinates.
 */
static void ptarray_to_wkt_sb(const POINTARRAY *ptarray, stringbuffer_t *sb, int precision, uint8_t variant) {
	/* OGC only includes X/Y */
	uint32_t dimensions = 2;

	/* ISO and extended formats include all dimensions */
	if (variant & (WKT_ISO | WKT_EXTENDED))
		dimensions = FLAGS_NDIMS(ptarray->flags);

	stringbuffer_makeroom(sb, 2 + ((OUT_MAX_BYTES_DOUBLE + 1) * dimensions * ptarray->npoints));
	/* Opening paren? */
	if (!(variant & WKT_NO_PARENS))
		stringbuffer_append_len(sb, "(", 1);

	/* Digits and commas */
	if (ptarray->npoints) {
		uint32_t i = 0;

		double *dbl_ptr = (double *)getPoint_internal(ptarray, i);
		coordinate_to_wkt_sb(dbl_ptr, sb, dimensions, precision);

		for (i = 1; i < ptarray->npoints; i++) {
			stringbuffer_append_len(sb, ",", 1);
			dbl_ptr = (double *)getPoint_internal(ptarray, i);
			coordinate_to_wkt_sb(dbl_ptr, sb, dimensions, precision);
		}
	}

	/* Closing paren? */
	if (!(variant & WKT_NO_PARENS))
		stringbuffer_append_len(sb, ")", 1);
}

/*
 * A four-dimensional point will have different outputs depending on variant.
 *   ISO: POINT ZM (0 0 0 0)
 *   Extended: POINT(0 0 0 0)
 *   OGC: POINT(0 0)
 * A three-dimensional m-point will have different outputs too.
 *   ISO: POINT M (0 0 0)
 *   Extended: POINTM(0 0 0)
 *   OGC: POINT(0 0)
 */
static void lwpoint_to_wkt_sb(const LWPOINT *pt, stringbuffer_t *sb, int precision, uint8_t variant) {
	if (!(variant & WKT_NO_TYPE)) {
		stringbuffer_append_len(sb, "POINT", 5); /* "POINT" */
		dimension_qualifiers_to_wkt_sb((LWGEOM *)pt, sb, variant);
	}

	if (lwpoint_is_empty(pt)) {
		empty_to_wkt_sb(sb);
		return;
	}

	ptarray_to_wkt_sb(pt->point, sb, precision, variant);
}

/*
 * Generic GEOMETRY
 */
static void lwgeom_to_wkt_sb(const LWGEOM *geom, stringbuffer_t *sb, int precision, uint8_t variant) {

	switch (geom->type) {
	case POINTTYPE:
		lwpoint_to_wkt_sb((LWPOINT *)geom, sb, precision, variant);
		break;
		// Need to do with postgis

	default:
		// lwerror("lwgeom_to_wkt_sb: Type %d - %s unsupported.",
		//         geom->type, lwtype_name(geom->type));
		return;
	}
}

static stringbuffer_t *lwgeom_to_wkt_internal(const LWGEOM *geom, uint8_t variant, int precision) {
	stringbuffer_t *sb;
	if (geom == NULL)
		return NULL;
	sb = stringbuffer_create();
	/* Extended mode starts with an "SRID=" section for geoms that have one */
	if ((variant & WKT_EXTENDED) && lwgeom_has_srid(geom)) {
		stringbuffer_aprintf(sb, "SRID=%d;", geom->srid);
	}
	lwgeom_to_wkt_sb(geom, sb, precision, variant);
	if (stringbuffer_getstring(sb) == NULL) {
		// lwerror("Uh oh");
		return NULL;
	}
	return sb;
}

/**
 * WKT emitter function. Allocates a new *char and fills it with the WKT
 * representation. If size_out is not NULL, it will be set to the size of the
 * allocated *char.
 *
 * @param variant Bitmasked value, accepts one of WKT_ISO, WKT_SFSQL,
 * WKT_EXTENDED.
 * @param precision Maximal number of digits after comma in the output doubles.
 * @param size_out If supplied, will return the size of the returned string,
 * including the null terminator.
 */
char *lwgeom_to_wkt(const LWGEOM *geom, uint8_t variant, int precision, size_t *size_out) {
	stringbuffer_t *sb = lwgeom_to_wkt_internal(geom, variant, precision);
	if (!sb)
		return NULL;
	char *str = stringbuffer_getstringcopy(sb);
	if (size_out)
		*size_out = stringbuffer_getlength(sb) + 1;
	stringbuffer_destroy(sb);
	return str;
}

} // namespace duckdb