#include "postgis/lwgeom_geos.hpp"

#include "liblwgeom/gserialized.hpp"
#include "libpgcommon/lwgeom_pg.hpp"

namespace duckdb {

GSERIALIZED *centroid(GSERIALIZED *geom) {
	GSERIALIZED *result;
	LWGEOM *lwgeom = lwgeom_from_gserialized(geom);

	result = geometry_serialize(lwgeom);
	lwgeom_free(lwgeom);

	return result;
}

} // namespace duckdb
