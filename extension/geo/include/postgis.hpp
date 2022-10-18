#pragma once

#include "duckdb/common/constants.hpp"
#include "liblwgeom/liblwgeom.hpp"

#include <iostream>
#include <string>
#include <vector>

namespace duckdb {
class Postgis {
public:
	Postgis();
	~Postgis();

public:
	GSERIALIZED *LWGEOM_in(char *input);
	GSERIALIZED *LWGEOM_getGserialized(const void *base, size_t size);
	idx_t LWGEOM_size(GSERIALIZED *gser);
	char *LWGEOM_base(GSERIALIZED *gser);
	string LWGEOM_asText(const void *data, size_t size);
	string LWGEOM_asBinary(const void *data, size_t size);
	string LWGEOM_asGeoJson(const void *data, size_t size);
	void LWGEOM_free(GSERIALIZED *gser);

	GSERIALIZED *LWGEOM_makepoint(double x, double y);
	GSERIALIZED *LWGEOM_makepoint(double x, double y, double z);
	double ST_distance(GSERIALIZED *geom1, GSERIALIZED *geom2);
	double geography_distance(GSERIALIZED *geom1, GSERIALIZED *geom2, bool use_spheroid);
	GSERIALIZED *LWGEOM_from_text(char *text, int srid = 0);
	GSERIALIZED *LWGEOM_from_WKB(const char *bytea_wkb, size_t byte_size, int srid = 0);
	double LWGEOM_x_point(const void *data, size_t size);
	GSERIALIZED *centroid(GSERIALIZED *geom);
	GSERIALIZED *geography_centroid(GSERIALIZED *geom, bool use_spheroid);
};
} // namespace duckdb
