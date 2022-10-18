#include "postgis.hpp"

#include "postgis/geography_measurement.hpp"
#include "postgis/lwgeom_functions_basic.hpp"
#include "postgis/lwgeom_inout.hpp"
#include "postgis/lwgeom_ogc.hpp"
#include "postgis/lwgeom_geos.hpp"
#include "postgis/geography_centroid.hpp"

namespace duckdb {
Postgis::Postgis() {
}

Postgis::~Postgis() {
}

GSERIALIZED *Postgis::LWGEOM_in(char *input) {
	return duckdb::LWGEOM_in(input);
}

GSERIALIZED *Postgis::LWGEOM_getGserialized(const void *base, size_t size) {
	return duckdb::LWGEOM_getGserialized(base, size);
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

GSERIALIZED *Postgis::LWGEOM_makepoint(double x, double y) {
	return duckdb::LWGEOM_makepoint(x, y);
}

GSERIALIZED *Postgis::LWGEOM_makepoint(double x, double y, double z) {
	return duckdb::LWGEOM_makepoint(x, y, z);
}

double Postgis::ST_distance(GSERIALIZED *geom1, GSERIALIZED *geom2) {
	return duckdb::ST_distance(geom1, geom2);
}

double Postgis::geography_distance(GSERIALIZED *geom1, GSERIALIZED *geom2, bool use_spheroid) {
	return duckdb::geography_distance(geom1, geom2, use_spheroid);
}

GSERIALIZED *Postgis::LWGEOM_from_text(char *text, int srid) {
	return duckdb::LWGEOM_from_text(text, srid);
}

GSERIALIZED *Postgis::LWGEOM_from_WKB(const char *bytea_wkb, size_t byte_size, int srid) {
	return duckdb::LWGEOM_from_WKB(bytea_wkb, byte_size, srid);
}

double Postgis::LWGEOM_x_point(const void *data, size_t size) {
	return duckdb::LWGEOM_x_point(data, size);
}

GSERIALIZED *Postgis::centroid(GSERIALIZED *geom) {
	return duckdb::centroid(geom);
}

GSERIALIZED *Postgis::geography_centroid(GSERIALIZED *geom, bool use_spheroid) {
	return duckdb::geography_centroid(geom, use_spheroid);
}

} // namespace duckdb
