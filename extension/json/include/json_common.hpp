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

static inline bool ConvertToPath(const string_t &input, string &result, idx_t &len) {
	len = input.GetSize();
	if (len == 0) {
		return false;
	}
	const char *ptr = input.GetDataUnsafe();
	if (*ptr == '/') {
		// Already a path string
		result = input.GetString();
	} else if (*ptr == '$') {
		// Dollar/dot/brackets syntax
		result = StringUtil::Replace(string(ptr + 1, len - 1), ".", "/");
		result = StringUtil::Replace(result, "]", "");
		result = StringUtil::Replace(result, "[", "/");
		len = result.length();
	} else {
		// Plain tag/array index, prepend slash
		len++;
		result = "/" + input.GetString();
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
