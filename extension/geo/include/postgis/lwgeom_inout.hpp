#pragma once

#include "liblwgeom/liblwgeom.hpp"

namespace duckdb {
/*
 * LWGEOM_in(cstring)
 * format is '[SRID=#;]wkt|wkb'
 *  LWGEOM_in( 'SRID=99;POINT(0 0)')
 *  LWGEOM_in( 'POINT(0 0)')            --> assumes SRID=SRID_UNKNOWN
 *  LWGEOM_in( 'SRID=99;0101000000000000000000F03F000000000000004')
 *  LWGEOM_in( '0101000000000000000000F03F000000000000004')
 *  LWGEOM_in( '{"type":"Point","coordinates":[1,1]}')
 *  returns a GSERIALIZED object
 */
GSERIALIZED *LWGEOM_in(char *input);

size_t LWGEOM_size(GSERIALIZED *gser);
char *LWGEOM_base(GSERIALIZED *gser);
std::string LWGEOM_asText(const void *base, size_t size);
std::string LWGEOM_asBinary(const void *base, size_t size);
std::string LWGEOM_asGeoJson(const void* base, size_t size);
void LWGEOM_free(GSERIALIZED *gser);

} // namespace duckdb
