#include "postgis.hpp"
#include "liblwgeom/liblwgeom.hpp"
#include "postgis/lwgeom_inout.hpp"
#include "postgis/lwgeom_functions_basic.hpp"
#include "postgis/lwgeom_ogc.hpp"
#include "postgis/geography_measurement.cpp"
#include "duckdb/common/constants.hpp"

namespace duckdb {

Postgis::Postgis() : success(false), error_message(""), error_location(0) {
}

Postgis::~Postgis() {
}

duckdb_postgis::GSERIALIZED* Postgis::LWGEOM_in(char* input) {
    auto ger = duckdb_postgis::LWGEOM_in(input);
    return ger;
}

duckdb_postgis::GSERIALIZED* Postgis::LWGEOM_getGserialized(const void* base, size_t size) {
    return duckdb_postgis::LWGEOM_getGserialized(base, size);
}

idx_t Postgis::LWGEOM_size(duckdb_postgis::GSERIALIZED *gser) {
    return duckdb_postgis::LWGEOM_size(gser);
}

char *Postgis::LWGEOM_base(duckdb_postgis::GSERIALIZED *gser) {
    return duckdb_postgis::LWGEOM_base(gser);
}

string Postgis::LWGEOM_asText(const void* data, size_t size) {
    return duckdb_postgis::LWGEOM_asText(data, size);
}

string Postgis::LWGEOM_asBinary(const void* data, size_t size) {
    return duckdb_postgis::LWGEOM_asBinary(data, size);
}

string Postgis::LWGEOM_asGeoJson(const void* data, size_t size) {
    return duckdb_postgis::LWGEOM_asGeoJson(data, size);
}

void Postgis::LWGEOM_free(duckdb_postgis::GSERIALIZED *gser) {
    duckdb_postgis::LWGEOM_free(gser);
}

duckdb_postgis::GSERIALIZED* Postgis::LWGEOM_makepoint(double x, double y) {
    return duckdb_postgis::LWGEOM_makepoint(x, y);
}

duckdb_postgis::GSERIALIZED* Postgis::LWGEOM_makepoint(double x, double y, double z) {
    return duckdb_postgis::LWGEOM_makepoint(x, y, z);
}

duckdb_postgis::GSERIALIZED* Postgis::LWGEOM_from_text(char* text, int srid) {
    return duckdb_postgis::LWGEOM_from_text(text, srid);
}

duckdb_postgis::GSERIALIZED* Postgis::LWGEOM_from_WKB(char *bytea_wkb, int srid) {
    return duckdb_postgis::LWGEOM_from_WKB(bytea_wkb, srid);
}

double Postgis::LWGEOM_x_point(const void* data, size_t size) {
    return duckdb_postgis::LWGEOM_x_point(data, size);
}

double Postgis::ST_distance(duckdb_postgis::GSERIALIZED *geom1, duckdb_postgis::GSERIALIZED *geom2) {
    return duckdb_postgis::ST_distance(geom1, geom2);
}

double Postgis::geography_distance(duckdb_postgis::GSERIALIZED *geom1, duckdb_postgis::GSERIALIZED *geom2, bool use_spheroid) {
    return duckdb_postgis::geography_distance(geom1, geom2, use_spheroid);
}

}
