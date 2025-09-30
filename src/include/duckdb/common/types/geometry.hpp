//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/time.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

enum class GeometryType : uint32_t {
	INVALID = 0,
	POINT = 1,
	LINESTRING = 2,
	POLYGON = 3,
	MULTIPOINT = 4,
	MULTILINESTRING = 5,
	MULTIPOLYGON = 6,
	GEOMETRYCOLLECTION = 7,
};

class Geometry {
public:
	static constexpr auto MAX_RECURSION_DEPTH = 16;

	DUCKDB_API static bool FromString(const string_t &wkt_text, string_t &result, Vector &result_vector, bool strict);
	DUCKDB_API static string_t ToString(Vector &result, const string_t &geom);
};

} // namespace duckdb
