#include "postgis/lwgeom_inout.hpp"

#include "liblwgeom/gserialized.hpp"
#include "liblwgeom/liblwgeom_internal.hpp"
#include "libpgcommon/lwgeom_pg.hpp"

#include <cstring>
#include <string>

namespace duckdb {

GSERIALIZED *LWGEOM_in(char *input) {
	char *str = input;
	LWGEOM_PARSER_RESULT lwg_parser_result;
	LWGEOM *lwgeom;
	GSERIALIZED *ret;
	int32_t srid = 0;

	lwgeom_parser_result_init(&lwg_parser_result);

	/* Starts with "SRID=" */
	if (strncasecmp(str, "SRID=", 5) == 0) {
		/* Roll forward to semi-colon */
		char *tmp = str;
		while (tmp && *tmp != ';')
			tmp++;

		/* Check next character to see if we have WKB  */
		if (tmp && *(tmp + 1) == '0') {
			/* Null terminate the SRID= string */
			*tmp = '\0';
			/* Set str to the start of the real WKB */
			str = tmp + 1;
			/* Move tmp to the start of the numeric part */
			tmp = input + 5;
			/* Parse out the SRID number */
			srid = atoi(tmp);
		}
	}

	/* WKB? Let's find out. */
	if (str[0] == '0') {
		size_t hexsize = strlen(str);
		unsigned char *wkb = bytes_from_hexbytes(str, hexsize);
		/* TODO: 20101206: No parser checks! This is inline with current 1.5 behavior, but needs discussion */
		lwgeom = lwgeom_from_wkb(wkb, hexsize / 2, LW_PARSER_CHECK_NONE);
		if (!lwgeom) {
			return NULL;
		}
		/* If we picked up an SRID at the head of the WKB set it manually */
		char *wkt = lwgeom_to_wkt(lwgeom, WKT_EXTENDED, 15, NULL);
		if (srid)
			lwgeom_set_srid(lwgeom, srid);
		/* Add a bbox if necessary */
		if (lwgeom_needs_bbox(lwgeom))
			lwgeom_add_bbox(lwgeom);
		lwfree(wkb);
		ret = geometry_serialize(lwgeom);
		lwgeom_free(lwgeom);
	}
	/* GEOJson */
	else if (str[0] == '{') {
		char *srs = NULL;
		lwgeom = lwgeom_from_geojson(str, &srs);
		if (!lwgeom) {
			return NULL;
		} else {
		}
		auto buffer = lwgeom_to_wkb_buffer(lwgeom, WKB_EXTENDED);
		auto buf_size = lwgeom_to_wkb_size(lwgeom, WKB_EXTENDED);
		char *wkt = lwgeom_to_wkt(lwgeom, WKT_EXTENDED, 15, NULL);
		ret = geometry_serialize(lwgeom);
		lwgeom_free(lwgeom);
	}
	/* WKT then. */
	else {
		if (lwgeom_parse_wkt(&lwg_parser_result, str, LW_PARSER_CHECK_ALL) == LW_FAILURE) {
			// PG_PARSER_ERROR(lwg_parser_result);
			// PG_RETURN_NULL();
			return NULL;
		}
		lwgeom = lwg_parser_result.geom;
		if (!lwgeom) {
			return NULL;
		}
		LWPOINT *point = (LWPOINT *)lwgeom;
		auto buffer = lwgeom_to_wkb_buffer(lwgeom, WKB_EXTENDED);
		auto buf_size = lwgeom_to_wkb_size(lwgeom, WKB_EXTENDED);
		char *wkt = lwgeom_to_wkt(lwgeom, WKT_EXTENDED, 15, NULL);
		if (lwgeom_needs_bbox(lwgeom))
			lwgeom_add_bbox(lwgeom);
		ret = geometry_serialize(lwgeom);
		lwgeom_parser_result_free(&lwg_parser_result);
	}

	return ret;
}

GSERIALIZED *LWGEOM_getGserialized(const void *base, size_t size) {
	GSERIALIZED *ret;
	LWGEOM *lwgeom = lwgeom_from_wkb(static_cast<const uint8_t *>(base), size, LW_PARSER_CHECK_NONE);
	ret = geometry_serialize(lwgeom);
	lwgeom_free(lwgeom);
	return ret;
}

size_t LWGEOM_size(GSERIALIZED *gser) {
	LWGEOM *lwgeom = lwgeom_from_gserialized(gser);
	if (lwgeom == NULL) {
		return 0;
	}

	auto buf_size = lwgeom_to_wkb_size(lwgeom, WKB_EXTENDED);
	lwgeom_free(lwgeom);
	return buf_size;
}

char *LWGEOM_base(GSERIALIZED *gser) {
	LWGEOM *lwgeom = lwgeom_from_gserialized(gser);
	if (lwgeom == NULL) {
		return 0;
	}

	auto buffer = lwgeom_to_wkb_buffer(lwgeom, WKB_EXTENDED);
	lwgeom_free(lwgeom);
	return (char *)buffer;
}

std::string LWGEOM_asText(const void *base, size_t size) {
	std::string rstr = "";
	LWGEOM *lwgeom = lwgeom_from_wkb(static_cast<const uint8_t *>(base), size, LW_PARSER_CHECK_NONE);
	size_t wkt_size;
	rstr = lwgeom_to_wkt(lwgeom, WKT_EXTENDED, WKT_PRECISION, &wkt_size);
	lwgeom_free(lwgeom);
	return rstr;
}

std::string LWGEOM_asBinary(const void *base, size_t size) {
	std::string rstr = "";
	LWGEOM *lwgeom = lwgeom_from_wkb(static_cast<const uint8_t *>(base), size, LW_PARSER_CHECK_NONE);
	rstr = lwgeom_to_hexwkb_buffer(lwgeom, WKB_NDR | WKB_EXTENDED);
	lwgeom_free(lwgeom);
	return rstr;
}

std::string LWGEOM_asGeoJson(const void *base, size_t size) {
	std::string rstr = "";
	LWGEOM *lwgeom = lwgeom_from_wkb(static_cast<const uint8_t *>(base), size, LW_PARSER_CHECK_NONE);
	auto varlen = lwgeom_to_geojson(lwgeom, nullptr, OUT_DEFAULT_DECIMAL_DIGITS, 0);
	rstr = std::string(varlen->data);
	lwfree(varlen);
	lwgeom_free(lwgeom);
	return rstr;
}

void LWGEOM_free(GSERIALIZED *gser) {
	if (gser) {
		lwfree(gser);
	}
}

} // namespace duckdb
