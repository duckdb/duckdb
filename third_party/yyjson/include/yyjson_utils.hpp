//===----------------------------------------------------------------------===//
//                         DuckDB
//
// yyjson_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

struct ConvertedJSONHolder {
public:
	~ConvertedJSONHolder() {
		if (doc) {
			yyjson_mut_doc_free(doc);
		}
		if (stringified_json) {
			free(stringified_json);
		}
	}

public:
	yyjson_mut_doc *doc = nullptr;
	char *stringified_json = nullptr;
};

} // namespace duckdb
