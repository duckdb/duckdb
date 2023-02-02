//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar/strftime.hpp"
#include "json_common.hpp"

namespace duckdb {

struct DateFormatMap;

struct JSONTransformOptions {
public:
	//! Throws an error if the cast doesn't work (instead of NULL-ing it)
	bool strict_cast;
	//! Throws an error if there is a duplicate key (instead of ignoring it)
	bool error_duplicate_key;
	//! Throws an error if a key is missing (instead of NULL-ing it)
	bool error_missing_key;
	//! Throws an error if an object has a key we didn't know about
	bool error_unknown_key;
	//! Date format used for parsing
	DateFormatMap *date_format_map;

public:
	void Serialize(FieldWriter &writer);
	void Deserialize(FieldReader &reader);
};

struct TryParseDate {
	template <class T>
	static inline bool Operation(StrpTimeFormat &format, const string_t &input, T &result, string &error_message) {
		return format.TryParseDate(input, result, error_message);
	}
};

struct TryParseTimeStamp {
	template <class T>
	static inline bool Operation(StrpTimeFormat &format, const string_t &input, T &result, string &error_message) {
		return format.TryParseTimestamp(input, result, error_message);
	}
};

struct JSONTransform {
	static void TransformObject(yyjson_val *objects[], yyjson_alc *alc, const idx_t count, const vector<string> &names,
	                            const vector<Vector *> &result_vectors, const JSONTransformOptions &options);
	static void GetStringVector(yyjson_val *vals[], const idx_t count, const LogicalType &target, Vector &string_vector,
	                            const bool strict);
};

} // namespace duckdb
