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
	static void MakePointFunction(DataChunk &args, ExpressionState &state, Vector &result);
	static void GeometryAsTextFunction(DataChunk &args, ExpressionState &state, Vector &result);
	static void GeometryDistanceFunction(DataChunk &args, ExpressionState &state, Vector &result);
	static void GeometryCentroidFunction(DataChunk &args, ExpressionState &state, Vector &result);
	static void GeometryFromTextFunction(DataChunk &args, ExpressionState &state, Vector &result);
	static void GeometryFromWKBFunction(DataChunk &args, ExpressionState &state, Vector &result);
	static void GeometryGetXFunction(DataChunk &args, ExpressionState &state, Vector &result);
};

} // namespace duckdb
