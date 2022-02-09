#pragma once

#include <string>
#include <vector>
#include <iostream>

#include "liblwgeom/liblwgeom.hpp"
#include "duckdb/common/constants.hpp"

namespace duckdb {
class Postgis {
public:
	Postgis();
	~Postgis();

	bool success;
	std::string error_message;
	int error_location;

public:
	duckdb_postgis::GSERIALIZED* LWGEOM_in(char* input);
	duckdb_postgis::GSERIALIZED* LWGEOM_getGserialized(const void* base, size_t size);
	idx_t LWGEOM_size(duckdb_postgis::GSERIALIZED *gser);
	char *LWGEOM_base(duckdb_postgis::GSERIALIZED *gser);
	string LWGEOM_asText(const void* data, size_t size);
	string LWGEOM_asBinary(const void* data, size_t size);
	string LWGEOM_asGeoJson(const void* data, size_t size);
	void LWGEOM_free(duckdb_postgis::GSERIALIZED *gser);
	duckdb_postgis::GSERIALIZED* LWGEOM_makepoint(double x, double y);
	duckdb_postgis::GSERIALIZED* LWGEOM_makepoint(double x, double y, double z);
	duckdb_postgis::GSERIALIZED* LWGEOM_from_text(char* text, int srid = 0);
	duckdb_postgis::GSERIALIZED* LWGEOM_from_WKB(char *bytea_wkb, int srid = 0);
	double LWGEOM_x_point(const void* data, size_t size);
	double ST_distance(duckdb_postgis::GSERIALIZED *geom1, duckdb_postgis::GSERIALIZED *geom2);
	double geography_distance(duckdb_postgis::GSERIALIZED *geom1, duckdb_postgis::GSERIALIZED *geom2, bool use_spheroid);
};
}