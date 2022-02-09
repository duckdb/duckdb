#include "liblwgeom/gserialized.hpp"
#include "postgis/lwgeom_ogc.hpp"
#include "libpgcommon/lwgeom_pg.hpp"

namespace duckdb_postgis
{

GSERIALIZED* LWGEOM_from_text(char* wkt, int srid)
{
	LWGEOM_PARSER_RESULT lwg_parser_result;
	GSERIALIZED *geom_result = NULL;
	LWGEOM *lwgeom;

	if (lwgeom_parse_wkt(&lwg_parser_result, wkt, LW_PARSER_CHECK_ALL) == LW_FAILURE )
		// PG_PARSER_ERROR(lwg_parser_result);
        return NULL;

	lwgeom = lwg_parser_result.geom;

	if ( lwgeom->srid != SRID_UNKNOWN )
	{
		// elog(WARNING, "OGC WKT expected, EWKT provided - use GeomFromEWKT() for this");
	}

	/* read user-requested SRID if any */
	if (srid != SRID_UNKNOWN)
		lwgeom_set_srid(lwgeom, srid);

	geom_result = geometry_serialize(lwgeom);
	lwgeom_parser_result_free(&lwg_parser_result);

	return geom_result;
}

GSERIALIZED* LWGEOM_from_WKB(char *bytea_wkb, int srid) {
	GSERIALIZED *geom;
	// LWGEOM *lwgeom;
	// uint8_t *wkb = (uint8_t*)VARDATA(bytea_wkb);

	// lwgeom = lwgeom_from_wkb(wkb, VARSIZE_ANY_EXHDR(bytea_wkb), LW_PARSER_CHECK_ALL);
	// if (!lwgeom)
	// 	// lwpgerror("Unable to parse WKB");
	// 	return NULL;

	// geom = geometry_serialize(lwgeom);
	// lwgeom_free(lwgeom);
	// PG_FREE_IF_COPY(bytea_wkb, 0);

	// if ( gserialized_get_srid(geom) != SRID_UNKNOWN )
	// {
	// 	// elog(WARNING, "OGC WKB expected, EWKB provided - use GeometryFromEWKB() for this");
	// }

	// if (srid != SRID_UNKNOWN)
	// {
	// 	if ( srid != gserialized_get_srid(geom) )
	// 		gserialized_set_srid(geom, srid);
	// }

	return geom;
}

double LWGEOM_x_point(const void* base, size_t size) {
	LWGEOM* lwgeom = lwgeom_from_wkb(static_cast<const uint8_t *>(base), size, LW_PARSER_CHECK_NONE);
	GSERIALIZED* geom = geometry_serialize(lwgeom);
	POINT4D pt;

	if (gserialized_get_type(geom) != POINTTYPE)
		// lwpgerror("Argument to ST_X() must have type POINT");
		return LW_FAILURE;

	if (gserialized_peek_first_point(geom, &pt) == LW_FAILURE)
	{
		return LW_FAILURE;
	}
	return pt.x;
}

} // duckdb_postgis