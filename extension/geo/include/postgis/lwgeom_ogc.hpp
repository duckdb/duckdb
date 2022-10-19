#pragma once
#include "duckdb.hpp"
#include "liblwgeom/liblwgeom.hpp"

namespace duckdb {

GSERIALIZED *LWGEOM_from_text(char *text, int srid = SRID_UNKNOWN);
GSERIALIZED *LWGEOM_from_WKB(const char *bytea_wkb, size_t byte_size, int srid = SRID_UNKNOWN);
double LWGEOM_x_point(const void *base, size_t size);

} // namespace duckdb
