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

struct JSONTransform {
	static void TransformObject(yyjson_val *objects[], yyjson_alc *alc, const idx_t count, const vector<string> &names,
	                            const vector<Vector *> &result_vectors, const bool strict);
};

} // namespace duckdb
