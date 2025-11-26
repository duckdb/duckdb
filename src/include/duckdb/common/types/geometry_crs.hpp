//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/geometry_crs.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/pair.hpp"
#include <limits>
#include <cmath>

namespace duckdb {

enum class CoordinateReferenceSystemType : uint8_t {
	// Empty
	INVALID = 0,
	// Opaque identifier
	SRID = 1,
	// PROJJSON format
	PROJJSON = 2,
	// WKT2_2019 format
	WKT2_2019 = 3,
	// AUTH:CODE format
	AUTH_CODE = 4,
};

class CoordinateReferenceSystem {
public:
	CoordinateReferenceSystem() = default;
	explicit CoordinateReferenceSystem(const string &crs);

	const string &GetDefinition() const {
		return text;
	}

	const string &GetName() const {
		return name;
	}

	CoordinateReferenceSystemType GetType() const {
		return type;
	}

	bool operator==(const CoordinateReferenceSystem &other) const {
		return type == other.type && text == other.text;
	}

	bool operator!=(const CoordinateReferenceSystem &other) const {
		return !(*this == other);
	}

private:
	//! The type of the coordinate reference system
	CoordinateReferenceSystemType type = CoordinateReferenceSystemType::INVALID;

	//! The text representation of the coordinate reference system
	//! E.g. "EPSG:4326", or a PROJJSON or WKT2 string
	string text;

	//! The "friendly" name of the coordinate reference system
	string name;
};

} // namespace duckdb
