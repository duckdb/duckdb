#include "postgis.hpp"

#include "postgis/lwgeom_inout.hpp"

namespace duckdb {
Postgis::Postgis() {
}

Postgis::~Postgis() {
}

GSERIALIZED *Postgis::LWGEOM_in(char *input) {
	return duckdb::LWGEOM_in(input);
}

char *Postgis::LWGEOM_base(GSERIALIZED *gser) {
	return duckdb::LWGEOM_base(gser);
}

string Postgis::LWGEOM_asBinary(const void *data, size_t size) {
	return duckdb::LWGEOM_asBinary(data, size);
}

string Postgis::LWGEOM_asText(const void *data, size_t size) {
	return duckdb::LWGEOM_asText(data, size);
}

string Postgis::LWGEOM_asGeoJson(const void *data, size_t size) {
	return duckdb::LWGEOM_asGeoJson(data, size);
}

idx_t Postgis::LWGEOM_size(GSERIALIZED *gser) {
	return duckdb::LWGEOM_size(gser);
}

void Postgis::LWGEOM_free(GSERIALIZED *gser) {
	duckdb::LWGEOM_free(gser);
}

} // namespace duckdb
