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
};
} // namespace duckdb
