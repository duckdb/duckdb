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
	//! Unknown
	INVALID = 0,
	//! Opaque identifier
	SRID = 1,
	//! PROJJSON format
	PROJJSON = 2,
	//! WKT2_2019 format
	WKT2_2019 = 3,
	//! AUTH:CODE format
	AUTH_CODE = 4,
};

class CoordinateReferenceSystem {
public:
	CoordinateReferenceSystem() = default;

	//! Equivalent to calling "TryParse" and throwing an exception on failure
	explicit CoordinateReferenceSystem(const string &crs);

	CoordinateReferenceSystem(CoordinateReferenceSystemType type, string text, string name, string code)
	    : type(type), text(std::move(text)), name(std::move(name)), code(std::move(code)) {
	}

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
	//! - Their codes match (if both have a code)
	//! - Their names match (if both have a name)
	//! - Their type and full text definitions match, character-for-character
	bool Equals(const CoordinateReferenceSystem &other) const {
		if (!code.empty() && code == other.code) {
			// Whatever the definitions are, if the codes match we consider them equal
			return true;
		}
		if (!name.empty() && name == other.name) {
			// Whatever the definitions are, if the names match we consider them equal
			return true;
		}
		// Finally, fall back to comparing the full definitions
		// This is not ideal, because the same CRS (in the same format!) can often be expressed in multiple ways
		// E.g. field order, whitespace differences, casing, etc. But it's better than nothing for now.

		// In the future we should:
		// 1. Implement proper normalization for each CRS format, and make _structured_ comparisons
		// 2. Allow extensions to inject a CRS handling library (e.g. PROJ) to perform proper _semantic_ comparisons
		return type == other.type && text == other.text;
	}

	void Serialize(Serializer &serializer) const;
	static CoordinateReferenceSystem Deserialize(Deserializer &deserializer);

public:
	static bool TryParse(const string &text, CoordinateReferenceSystem &result);
	static void Parse(const string &text, CoordinateReferenceSystem &result);

private:
	static bool TryParseAuthCode(const string &text, CoordinateReferenceSystem &result);
	static bool TryParseWKT2(const string &text, CoordinateReferenceSystem &result);
	static bool TryParsePROJJSON(const string &text, CoordinateReferenceSystem &result);

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

class CoordinateReferenceSystemLookupResult {
public:
	//! Return if the lookup was successful
	bool Success() const {
		return confidence != 0;
	}

	//! Get the confidence score (0-100) of the lookup
	//! A failed lookup always has confidence 0
	idx_t GetConfidenceScore() const {
		return confidence;
	}

	//! Get the result of the lookup
	//! Only valid if the lookup was successful
	CoordinateReferenceSystem &GetResult() {
		return crs;
	}

	void SetResult(CoordinateReferenceSystem crs_p, idx_t confidence_p = 100) {
		crs = std::move(crs_p);
		confidence = confidence_p;
	}

	void SetResult(const string &crs_str, idx_t confidence_p = 100) {
		crs = CoordinateReferenceSystem(crs_str);
		confidence = confidence_p;
	}

	//! Construct a failed lookup result
	CoordinateReferenceSystemLookupResult() : confidence(0) {
	}

	//! Construct a successful lookup result
	CoordinateReferenceSystemLookupResult(idx_t confidence, CoordinateReferenceSystem crs_p)
	    : confidence(confidence), crs(std::move(crs_p)) {
	}

	static CoordinateReferenceSystemLookupResult NotFound() {
		return CoordinateReferenceSystemLookupResult();
	}

	static CoordinateReferenceSystemLookupResult FromString(const string &text, idx_t confidence = 100) {
		return CoordinateReferenceSystemLookupResult(confidence, CoordinateReferenceSystem(text));
	}

	operator bool() { // NOLINT: allow implicit lookup to bool
		return confidence > 0;
	}

	bool operator<(const CoordinateReferenceSystemLookupResult &other) const {
		return confidence < other.confidence;
	}

private:
	idx_t confidence;
	CoordinateReferenceSystem crs;
};

class CoordinateReferencesSystemProvider {
public:
	//! Try to convert a CRS to another format
	virtual CoordinateReferenceSystemLookupResult TryConvert(const CoordinateReferenceSystem &source_crs,
	                                                         CoordinateReferenceSystemType target_type) = 0;

	virtual ~CoordinateReferencesSystemProvider() = default;
};

class CoordinateReferenceSystemUtil {
public:
	CoordinateReferenceSystemUtil();


	void AddProvider(shared_ptr<CoordinateReferencesSystemProvider> provider);

	static CoordinateReferenceSystemUtil &Get(ClientContext &context);

	//! Try to convert the CRS to another format
	CoordinateReferenceSystemLookupResult TryConvert(const CoordinateReferenceSystem &source_crs,
	                                                 CoordinateReferenceSystemType target_type) const;

	CoordinateReferenceSystemLookupResult TryConvert(const string &source_crs,
	                                                 CoordinateReferenceSystemType target_type) const;

	//! Try to identify a crs string into a fully defined CRS
	CoordinateReferenceSystemLookupResult TryIdentify(const string &source_crs);

	static CoordinateReferenceSystemLookupResult DefaultTryConvert(const CoordinateReferenceSystem &source_crs,
	                                                               CoordinateReferenceSystemType target_type);

	static CoordinateReferenceSystemLookupResult DefaultTryConvert(const string &source_crs,
	                                                               CoordinateReferenceSystemType target_type);

	static CoordinateReferenceSystemLookupResult DefaultTryIdentify(const string &source_crs);

private:
	vector<shared_ptr<CoordinateReferencesSystemProvider>> providers;
};

} // namespace duckdb
