//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "json_common.hpp"

namespace duckdb {

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

public:
	void Serialize(FieldWriter &writer);
	void Deserialize(FieldReader &reader);
};

struct JSONTransform {
	static void TransformObject(yyjson_val *objects[], yyjson_alc *alc, const idx_t count, const vector<string> &names,
	                            const vector<Vector *> &result_vectors, const JSONTransformOptions &options);
};

} // namespace duckdb
