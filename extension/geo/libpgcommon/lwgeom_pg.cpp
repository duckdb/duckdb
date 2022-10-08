#include "libpgcommon/lwgeom_pg.hpp"

#include "liblwgeom/gserialized.hpp"

namespace duckdb {

/**
 * Utility method to call the serialization and then set the
 * PgSQL varsize header appropriately with the serialized size.
 */
GSERIALIZED *geometry_serialize(LWGEOM *lwgeom) {
	size_t ret_size;
	GSERIALIZED *g;

	g = gserialized_from_lwgeom(lwgeom, &ret_size);
	SET_VARSIZE(g, ret_size);
	return g;
}

} // namespace duckdb
