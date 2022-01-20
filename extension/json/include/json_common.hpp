//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

//! Converts JSON query string to JSON path string
static inline bool ConvertToPath(const string_t &query, string &result, idx_t &len) {
	len = query.GetSize();
	if (len == 0) {
		return false;
	}
	const char *ptr = query.GetDataUnsafe();
	if (*ptr == '/') {
		// Already a path string
		result = query.GetString();
	} else if (*ptr == '$') {
		// Dollar/dot/brackets syntax
		result = StringUtil::Replace(string(ptr + 1, len - 1), ".", "/");
		result = StringUtil::Replace(result, "]", "");
		result = StringUtil::Replace(result, "[", "/");
		len = result.length();
	} else {
		// Plain tag/array index, prepend slash
		len++;
		result = "/" + query.GetString();
	}
	return true;
}

struct JSONFunctionData : public FunctionData {
public:
	explicit JSONFunctionData(bool constant, string path_p, idx_t len)
	    : constant(constant), path(move(path_p)), len(len) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<JSONFunctionData>(constant, path, len);
	}

public:
	const bool constant;
	const string path;
	const size_t len;
};

} // namespace duckdb
