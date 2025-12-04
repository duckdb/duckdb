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

class Serializer;
class Deserializer;

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

	//! Equivalent to calling "TryParse" and throwing an exception on failure
	explicit CoordinateReferenceSystem(const string &crs);

	//! Get the identified type of the coordinate reference system
	CoordinateReferenceSystemType GetType() const {
		return type;
	}

	//! Get the full provided definition of the coordinate reference system
	const string &GetDefinition() const {
		return text;
	}

	//! Get the "friendly name" of the coordinate reference system
	//! This can be empty if no name could be determined from the definition
	const string &GetName() const {
		return name;
	}

	//! Get the "code" of the coordinate reference system (e.g. "EPSG:4326")
	//! This can be empty if no id could be determined from the definition
	const string &GetCode() const {
		return code;
	}

	//! Get the best available "display" name for this CRS
	//! This is the first non-empty value of "code", "name" and "definition"
	const string &GetDisplayName() const {
		if (!code.empty()) {
			return code;
		}
		if (!name.empty()) {
			return name;
		}
		return text;
	}

	// Only compare the definition, as "type", "name" and "id" are derived from it
	bool operator==(const CoordinateReferenceSystem &other) const {
		return text == other.text;
	}

	bool operator!=(const CoordinateReferenceSystem &other) const {
		return !(*this == other);
	}

	void Serialize(Serializer &serializer) const;
	static CoordinateReferenceSystem Deserialize(Deserializer &deserializer);

public:
	static bool TryParseAuthCode(const string &text, CoordinateReferenceSystem &result);
	static bool TryParseWKT2(const string &text, CoordinateReferenceSystem &result);
	static bool TryParsePROJJSON(const string &text, CoordinateReferenceSystem &result);
	static bool TryParse(const string &text, CoordinateReferenceSystem &result);

private:
	//! The type of the coordinate reference system
	CoordinateReferenceSystemType type = CoordinateReferenceSystemType::INVALID;

	//! The text definition of the coordinate reference system
	//! E.g. "AUTH:CODE", or a PROJJSON or WKT2 string
	string text;

	//! The "friendly name" of the coordinate reference system
	//! This can often be extracted from the definition, but is cached here for convenience
	string name;

	//! The "code" of the coordinate reference system (e.g. "EPSG:4326")
	//! This can often be extracted from the definition, but is cached here for convenience
	string code;
};

} // namespace duckdb
