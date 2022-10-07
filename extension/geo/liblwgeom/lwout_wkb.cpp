#include "liblwgeom/lwgeom_wkt.hpp"
#include "liblwgeom/lwinline.hpp"

#include <cstring>
#include <stddef.h> // for ptrdiff_t

namespace duckdb {

/*
 * Look-up table for hex writer
 */
static char const *hexchr = "0123456789ABCDEF";

static uint8_t *lwgeom_to_wkb_buf(const LWGEOM *geom, uint8_t *buf, uint8_t variant);

/*
 * Optional SRID
 */
static int lwgeom_wkb_needs_srid(const LWGEOM *geom, uint8_t variant) {
	/* Sub-components of collections inherit their SRID from the parent.
	   We force that behavior with the WKB_NO_SRID flag */
	if (variant & WKB_NO_SRID)
		return LW_FALSE;

	/* We can only add an SRID if the geometry has one, and the
	   WKB form is extended */
	if ((variant & WKB_EXTENDED) && lwgeom_has_srid(geom))
		return LW_TRUE;

	/* Everything else doesn't get an SRID */
	return LW_FALSE;
}

static uint8_t *double_nan_to_wkb_buf(uint8_t *buf, uint8_t variant) {
#define NAN_SIZE 8
	const uint8_t ndr_nan[NAN_SIZE] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x7f};
	const uint8_t xdr_nan[NAN_SIZE] = {0x7f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

	if (variant & WKB_HEX) {
		for (int i = 0; i < NAN_SIZE; i++) {
			uint8_t b = (variant & WKB_NDR) ? ndr_nan[i] : xdr_nan[i];
			/* Top four bits to 0-F */
			buf[2 * i] = hexchr[b >> 4];
			/* Bottom four bits to 0-F */
			buf[2 * i + 1] = hexchr[b & 0x0F];
		}
		return buf + (2 * NAN_SIZE);
	} else {
		for (int i = 0; i < NAN_SIZE; i++) {
			buf[i] = (variant & WKB_NDR) ? ndr_nan[i] : xdr_nan[i];
			;
		}
		return buf + NAN_SIZE;
	}
}

/*
 * GeometryType
 */
static uint32_t lwgeom_wkb_type(const LWGEOM *geom, uint8_t variant) {
	uint32_t wkb_type = 0;

	switch (geom->type) {
	case POINTTYPE:
		wkb_type = WKB_POINT_TYPE;
		break;
		// Need to do with postgis

	default:
		// lwerror("%s: Unsupported geometry type: %s", __func__, lwtype_name(geom->type));
		wkb_type = 0;
	}

	if (variant & WKB_EXTENDED) {
		if (FLAGS_GET_Z(geom->flags))
			wkb_type |= WKBZOFFSET;
		if (FLAGS_GET_M(geom->flags))
			wkb_type |= WKBMOFFSET;
		/*		if ( geom->srid != SRID_UNKNOWN && ! (variant & WKB_NO_SRID) ) */
		if (lwgeom_wkb_needs_srid(geom, variant))
			wkb_type |= WKBSRIDFLAG;
	} else if (variant & WKB_ISO) {
		/* Z types are in the 1000 range */
		if (FLAGS_GET_Z(geom->flags))
			wkb_type += 1000;
		/* M types are in the 2000 range */
		if (FLAGS_GET_M(geom->flags))
			wkb_type += 2000;
		/* ZM types are in the 1000 + 2000 = 3000 range, see above */
	}
	return wkb_type;
}

/*
 * Endian
 */
static uint8_t *endian_to_wkb_buf(uint8_t *buf, uint8_t variant) {
	if (variant & WKB_HEX) {
		buf[0] = '0';
		buf[1] = ((variant & WKB_NDR) ? '1' : '0');
		return buf + 2;
	} else {
		buf[0] = ((variant & WKB_NDR) ? 1 : 0);
		return buf + 1;
	}
}

/*
 * SwapBytes?
 */
static inline int wkb_swap_bytes(uint8_t variant) {
	/* If requested variant matches machine arch, we don't have to swap! */
	if (((variant & WKB_NDR) && !IS_BIG_ENDIAN) || ((!(variant & WKB_NDR)) && IS_BIG_ENDIAN)) {
		return LW_FALSE;
	}
	return LW_TRUE;
}

/*
 * Integer32
 */
static uint8_t *integer_to_wkb_buf(const uint32_t ival, uint8_t *buf, uint8_t variant) {
	uint8_t *iptr = (uint8_t *)(&ival);
	int i = 0;

	if (sizeof(int) != WKB_INT_SIZE) {
		// lwerror("Machine int size is not %d bytes!", WKB_INT_SIZE);
		return NULL;
	}
	if (variant & WKB_HEX) {
		int swap = wkb_swap_bytes(variant);
		/* Machine/request arch mismatch, so flip byte order */
		for (i = 0; i < WKB_INT_SIZE; i++) {
			int j = (swap ? WKB_INT_SIZE - 1 - i : i);
			uint8_t b = iptr[j];
			/* Top four bits to 0-F */
			buf[2 * i] = hexchr[b >> 4];
			/* Bottom four bits to 0-F */
			buf[2 * i + 1] = hexchr[b & 0x0F];
		}
		return buf + (2 * WKB_INT_SIZE);
	} else {
		/* Machine/request arch mismatch, so flip byte order */
		if (wkb_swap_bytes(variant)) {
			for (i = 0; i < WKB_INT_SIZE; i++) {
				buf[i] = iptr[WKB_INT_SIZE - 1 - i];
			}
		}
		/* If machine arch and requested arch match, don't flip byte order */
		else {
			memcpy(buf, iptr, WKB_INT_SIZE);
		}
		return buf + WKB_INT_SIZE;
	}
}

/*
 * Float64
 */
static uint8_t *double_to_wkb_buf(const double d, uint8_t *buf, uint8_t variant) {
	uint8_t *dptr = (uint8_t *)(&d);
	int i = 0;

	if (sizeof(double) != WKB_DOUBLE_SIZE) {
		// lwerror("Machine double size is not %d bytes!", WKB_DOUBLE_SIZE);
		return NULL;
	}

	if (variant & WKB_HEX) {
		int swap = wkb_swap_bytes(variant);
		/* Machine/request arch mismatch, so flip byte order */
		for (i = 0; i < WKB_DOUBLE_SIZE; i++) {
			int j = (swap ? WKB_DOUBLE_SIZE - 1 - i : i);
			uint8_t b = dptr[j];
			/* Top four bits to 0-F */
			buf[2 * i] = hexchr[b >> 4];
			/* Bottom four bits to 0-F */
			buf[2 * i + 1] = hexchr[b & 0x0F];
		}
		return buf + (2 * WKB_DOUBLE_SIZE);
	} else {
		/* Machine/request arch mismatch, so flip byte order */
		if (wkb_swap_bytes(variant)) {
			for (i = 0; i < WKB_DOUBLE_SIZE; i++) {
				buf[i] = dptr[WKB_DOUBLE_SIZE - 1 - i];
			}
		}
		/* If machine arch and requested arch match, don't flip byte order */
		else {
			memcpy(buf, dptr, WKB_DOUBLE_SIZE);
		}
		return buf + WKB_DOUBLE_SIZE;
	}
}

/**
 * Convert LWGEOM to a char* in WKB format. Caller is responsible for freeing
 * the returned array.
 *
 * @param variant. Unsigned bitmask value. Accepts one of: WKB_ISO, WKB_EXTENDED, WKB_SFSQL.
 * Accepts any of: WKB_NDR, WKB_HEX. For example: Variant = ( WKB_ISO | WKB_NDR ) would
 * return the little-endian ISO form of WKB. For Example: Variant = ( WKB_EXTENDED | WKB_HEX )
 * would return the big-endian extended form of WKB, as hex-encoded ASCII (the "canonical form").
 * @param size_out If supplied, will return the size of the returned memory segment,
 * including the null terminator in the case of ASCII.
 */
static ptrdiff_t lwgeom_to_wkb_write_buf(const LWGEOM *geom, uint8_t variant, uint8_t *buffer) {
	/* If neither or both variants are specified, choose the native order */
	if (!(variant & WKB_NDR || variant & WKB_XDR) || (variant & WKB_NDR && variant & WKB_XDR)) {
		if (IS_BIG_ENDIAN)
			variant = variant | WKB_XDR;
		else
			variant = variant | WKB_NDR;
	}

	/* Write the WKB into the output buffer */
	int written_bytes = (lwgeom_to_wkb_buf(geom, buffer, variant) - buffer);

	return written_bytes;
}

uint8_t *lwgeom_to_wkb_buffer(const LWGEOM *geom, uint8_t variant) {
	size_t b_size = lwgeom_to_wkb_size(geom, variant);
	/* Hex string takes twice as much space as binary + a null character */
	if (variant & WKB_HEX) {
		b_size = 2 * b_size + 1;
	}

	uint8_t *buffer = (uint8_t *)lwalloc(b_size);
	ptrdiff_t written_size = lwgeom_to_wkb_write_buf(geom, variant, buffer);
	if (variant & WKB_HEX) {
		buffer[written_size] = '\0';
		written_size++;
	}

	if (written_size != (ptrdiff_t)b_size) {
		char *wkt = lwgeom_to_wkt(geom, WKT_EXTENDED, 15, NULL);
		// lwerror("Output WKB is not the same size as the allocated buffer. Variant: %u, Geom: %s", variant, wkt);
		lwfree(wkt);
		lwfree(buffer);
		return NULL;
	}

	return buffer;
}

char *lwgeom_to_hexwkb_buffer(const LWGEOM *geom, uint8_t variant) {
	return (char *)lwgeom_to_wkb_buffer(geom, variant | WKB_HEX);
}

/*
 * Empty
 */
static size_t empty_to_wkb_size(const LWGEOM *geom, uint8_t variant) {
	/* endian byte + type integer */
	size_t size = WKB_BYTE_SIZE + WKB_INT_SIZE;

	/* optional srid integer */
	if (lwgeom_wkb_needs_srid(geom, variant))
		size += WKB_INT_SIZE;

	/* Represent POINT EMPTY as POINT(NaN NaN) */
	if (geom->type == POINTTYPE) {
		const LWPOINT *pt = (LWPOINT *)geom;
		size += WKB_DOUBLE_SIZE * FLAGS_NDIMS(pt->point->flags);
	}
	/* num-elements */
	else {
		size += WKB_INT_SIZE;
	}

	return size;
}

static uint8_t *empty_to_wkb_buf(const LWGEOM *geom, uint8_t *buf, uint8_t variant) {
	uint32_t wkb_type = lwgeom_wkb_type(geom, variant);

	/* Set the endian flag */
	buf = endian_to_wkb_buf(buf, variant);

	/* Set the geometry type */
	buf = integer_to_wkb_buf(wkb_type, buf, variant);

	/* Set the SRID if necessary */
	if (lwgeom_wkb_needs_srid(geom, variant))
		buf = integer_to_wkb_buf(geom->srid, buf, variant);

	/* Represent POINT EMPTY as POINT(NaN NaN) */
	if (geom->type == POINTTYPE) {
		const LWPOINT *pt = (LWPOINT *)geom;
		for (int i = 0; i < FLAGS_NDIMS(pt->point->flags); i++) {
			buf = double_nan_to_wkb_buf(buf, variant);
		}
	}
	/* Everything else is flagged as empty using num-elements == 0 */
	else {
		/* Set nrings/npoints/ngeoms to zero */
		buf = integer_to_wkb_buf(0, buf, variant);
	}

	return buf;
}

/*
 * POINTARRAY
 */
static size_t ptarray_to_wkb_size(const POINTARRAY *pa, uint8_t variant) {
	int dims = 2;
	size_t size = 0;

	if (variant & (WKB_ISO | WKB_EXTENDED))
		dims = FLAGS_NDIMS(pa->flags);

	/* Include the npoints if it's not a POINT type) */
	if (!(variant & WKB_NO_NPOINTS))
		size += WKB_INT_SIZE;

	/* size of the double list */
	size += pa->npoints * dims * WKB_DOUBLE_SIZE;

	return size;
}

static uint8_t *ptarray_to_wkb_buf(const POINTARRAY *pa, uint8_t *buf, uint8_t variant) {
	uint32_t dims = 2;
	uint32_t pa_dims = FLAGS_NDIMS(pa->flags);
	uint32_t i, j;
	double *dbl_ptr;

	/* SFSQL is always 2-d. Extended and ISO use all available dimensions */
	if ((variant & WKB_ISO) || (variant & WKB_EXTENDED))
		dims = pa_dims;

	/* Set the number of points (if it's not a POINT type) */
	if (!(variant & WKB_NO_NPOINTS))
		buf = integer_to_wkb_buf(pa->npoints, buf, variant);

	/* Bulk copy the coordinates when: dimensionality matches, output format */
	/* is not hex, and output endian matches internal endian. */
	if (pa->npoints && (dims == pa_dims) && !wkb_swap_bytes(variant) && !(variant & WKB_HEX)) {
		size_t size = pa->npoints * dims * WKB_DOUBLE_SIZE;
		memcpy(buf, getPoint_internal(pa, 0), size);
		buf += size;
	}
	/* Copy coordinates one-by-one otherwise */
	else {
		for (i = 0; i < pa->npoints; i++) {
			dbl_ptr = (double *)getPoint_internal(pa, i);
			for (j = 0; j < dims; j++) {
				buf = double_to_wkb_buf(dbl_ptr[j], buf, variant);
			}
		}
	}
	return buf;
}

/*
 * POINT
 */
static size_t lwpoint_to_wkb_size(const LWPOINT *pt, uint8_t variant) {
	/* Endian flag + type number */
	size_t size = WKB_BYTE_SIZE + WKB_INT_SIZE;

	/* Only process empty at this level in the EXTENDED case */
	if ((variant & WKB_EXTENDED) && lwgeom_is_empty((LWGEOM *)pt))
		return empty_to_wkb_size((LWGEOM *)pt, variant);

	/* Extended WKB needs space for optional SRID integer */
	if (lwgeom_wkb_needs_srid((LWGEOM *)pt, variant))
		size += WKB_INT_SIZE;

	/* Points */
	size += ptarray_to_wkb_size(pt->point, variant | WKB_NO_NPOINTS);
	return size;
}

/*
 * GEOMETRY
 */
size_t lwgeom_to_wkb_size(const LWGEOM *geom, uint8_t variant) {
	size_t size = 0;

	if (geom == NULL) {
		// lwerror("Cannot convert NULL into WKB.");
		return 0;
	}

	/* Short circuit out empty geometries */
	if ((!(variant & WKB_EXTENDED)) && lwgeom_is_empty(geom)) {
		return empty_to_wkb_size(geom, variant);
	}

	switch (geom->type) {
	case POINTTYPE:
		size += lwpoint_to_wkb_size((LWPOINT *)geom, variant);
		break;
	// Need to do with postgis

	/* Unknown type! */
	default:
		// lwerror("%s: Unsupported geometry type: %s", __func__, lwtype_name(geom->type));
		return 0;
	}

	return size;
}

static uint8_t *lwpoint_to_wkb_buf(const LWPOINT *pt, uint8_t *buf, uint8_t variant) {
	/* Only process empty at this level in the EXTENDED case */
	if ((variant & WKB_EXTENDED) && lwgeom_is_empty((LWGEOM *)pt))
		return empty_to_wkb_buf((LWGEOM *)pt, buf, variant);

	/* Set the endian flag */
	buf = endian_to_wkb_buf(buf, variant);
	/* Set the geometry type */
	buf = integer_to_wkb_buf(lwgeom_wkb_type((LWGEOM *)pt, variant), buf, variant);
	/* Set the optional SRID for extended variant */
	if (lwgeom_wkb_needs_srid((LWGEOM *)pt, variant)) {
		buf = integer_to_wkb_buf(pt->srid, buf, variant);
	}
	/* Set the coordinates */
	buf = ptarray_to_wkb_buf(pt->point, buf, variant | WKB_NO_NPOINTS);
	return buf;
}

static uint8_t *lwgeom_to_wkb_buf(const LWGEOM *geom, uint8_t *buf, uint8_t variant) {

	/* Do not simplify empties when outputting to canonical form */
	if (lwgeom_is_empty(geom) && !(variant & WKB_EXTENDED))
		return empty_to_wkb_buf(geom, buf, variant);

	switch (geom->type) {
	case POINTTYPE:
		return lwpoint_to_wkb_buf((LWPOINT *)geom, buf, variant);

	// Need to do with postgis

	/* Unknown type! */
	default:
		// lwerror("%s: Unsupported geometry type: %s", __func__, lwtype_name(geom->type));
		return 0;
	}
	/* Return value to keep compiler happy. */
	return 0;
}

} // namespace duckdb