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
	//! Not set / invalid
	INVALID = 0,
	//! Opaque identifier
	SRID = 1,
	//! AUTH:CODE format
	AUTH_CODE = 2,
	//! PROJJSON format
	PROJJSON = 3,
	//! WKT2_2019 format
	WKT2_2019 = 4,
};

class CoordinateReferenceSystem {
	friend class CoordinateReferenceSystemProvider;

public:
	CoordinateReferenceSystem() = default;

	explicit CoordinateReferenceSystem(const string &definition) {
		ParseDefinition(definition, *this);
	}

	//! Get the identified type of the coordinate reference system
	CoordinateReferenceSystemType GetType() const {
		return type;
	}

	//! Get the full provided definition of the coordinate reference system
	const string &GetDefinition() const {
		return definition;
	}

	//! Get the "identifier" of the coordinate reference system (e.g. "EPSG:4326")
	//! This is the same as the definition for AUTH:CODE and SRID types
	const string &GetIdentifier() const {
		return identifier.empty() ? definition : identifier;
	}

	//! Is this a fully-defined coordinate system?
	bool IsComplete() const {
		switch (GetType()) {
		case CoordinateReferenceSystemType::PROJJSON:
		case CoordinateReferenceSystemType::WKT2_2019:
			return true;
		default:
			return false;
		}
	}

	//! Attempt to determine if this CRS is equivalent to another CRS
	//! This is currently not very precise, and may yield false negatives
	//! We consider two CRSs equal if one of the following is true:
	//! - Their identifiers match
	//! - Their type and full text definitions match, character-for-character
	bool Equals(const CoordinateReferenceSystem &other) const {
		// Whatever the definitions are, if the identifiers match we consider them equal
		if (!identifier.empty() && identifier == other.identifier) {
			return true;
		}

		// Fall back to comparing the full definitions
		// This is not ideal, because the same CRS (in the same format!) can often be expressed in multiple ways
		// E.g. field order, whitespace differences, casing, etc. But it's better than nothing for now.

		// In the future we should:
		// 1. Implement proper normalization for each CRS format, and make _structured_ comparisons
		// 2. Allow extensions to inject a CRS handling library (e.g. PROJ) to perform proper _semantic_ comparisons
		return type == other.type && definition == other.definition;
	}

	//! Try to identify a CRS from a string
	//! If the string is unable to be identified as one of the registered coordinates systems, and
	//! - IS NOT a complete CRS definition, returns nullptr
	//! - IS a complete CRS definition (e.g. PROJJSON or WKT2), returns the CRS as is.
	//! Otherwise, returns the identified CRS in the most compact form possible (AUTH:CODE > SRID > PROJJSON > WKT2)
	static unique_ptr<CoordinateReferenceSystem> TryIdentify(ClientContext &context, const string &source_crs);

	//! Try to convert the CRS to another format
	//! Returns nullptr if no conversion could be performed
	static unique_ptr<CoordinateReferenceSystem> TryConvert(ClientContext &context,
	                                                        const CoordinateReferenceSystem &source_crs,
	                                                        CoordinateReferenceSystemType target_type);

	//! Try to convert the CRS to another format
	//! Returns nullptr if no conversion could be performed
	static unique_ptr<CoordinateReferenceSystem> TryConvert(ClientContext &context, const string &source_crs,
	                                                        CoordinateReferenceSystemType target_type);

	//! Serialize this CRS to a binary format
	void Serialize(Serializer &serializer) const;

	//! Deserialize a CRS from a binary format
	static CoordinateReferenceSystem Deserialize(Deserializer &deserializer);

private:
	static void ParseDefinition(const string &text, CoordinateReferenceSystem &result);
	static bool TryParseAuthCode(const string &text, CoordinateReferenceSystem &result);
	static bool TryParseWKT2(const string &text, CoordinateReferenceSystem &result);
	static bool TryParsePROJJSON(const string &text, CoordinateReferenceSystem &result);

private:
	//! The type of the coordinate reference system
	CoordinateReferenceSystemType type = CoordinateReferenceSystemType::INVALID;

	//! The text definition of the coordinate reference system
	//! E.g. "AUTH:CODE", or a PROJJSON or WKT2 string
	string definition;

	//! The identifier code of the coordinate reference system
	//! This can usually be derived from the text definition, but is cached here for convenience
	string identifier;
};

} // namespace duckdb
