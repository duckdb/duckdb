//===----------------------------------------------------------------------===//
//                         DuckDB
//
// geo-functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

struct GeoFunctions {
	static bool CastVarcharToGEO(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static bool CastGeoToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
};

} // namespace duckdb
