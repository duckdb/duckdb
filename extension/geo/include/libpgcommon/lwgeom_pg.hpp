#pragma once

#include "liblwgeom/liblwgeom.hpp"

namespace duckdb {

/**
 * Utility method to call the serialization and then set the
 * PgSQL varsize header appropriately with the serialized size.
 */
GSERIALIZED *geometry_serialize(LWGEOM *lwgeom);

} // namespace duckdb