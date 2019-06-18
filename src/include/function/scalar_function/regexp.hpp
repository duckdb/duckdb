//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/regexp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"
#include "re2/re2.h"

namespace duckdb {

void regexp_matches_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                             BoundFunctionExpression &expr, Vector &result);
bool regexp_matches_matches_arguments(vector<SQLType> &arguments);
SQLType regexp_matches_get_return_type(vector<SQLType> &arguments);
unique_ptr<FunctionData> regexp_matches_get_bind_function(BoundFunctionExpression &expr, ClientContext &context);

struct RegexpMatchesBindData : public FunctionData {
	std::unique_ptr<RE2> constant_pattern;
	string range_min, range_max;
	bool range_success;

	RegexpMatchesBindData(std::unique_ptr<RE2> constant_pattern, string range_min, string range_max, bool range_success)
	    : constant_pattern(std::move(constant_pattern)), range_min(range_min), range_max(range_max),
	      range_success(range_success) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<RegexpMatchesBindData>(std::move(constant_pattern), range_min, range_max, range_success);
	}
};

class RegexpMatchesFunction {
public:
	static const char *GetName() {
		return "regexp_matches";
	}

	static scalar_function_t GetFunction() {
		return regexp_matches_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return regexp_matches_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return regexp_matches_get_return_type;
	}

	static bind_scalar_function_t GetBindFunction() {
		return regexp_matches_get_bind_function;
	}

	static dependency_function_t GetDependencyFunction() {
		return nullptr;
	}

	static bool HasSideEffects() {
		return false;
	}
};

void regexp_replace_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                             BoundFunctionExpression &expr, Vector &result);
bool regexp_replace_matches_arguments(vector<SQLType> &arguments);
SQLType regexp_replace_get_return_type(vector<SQLType> &arguments);

class RegexpReplaceFunction {
public:
	static const char *GetName() {
		return "regexp_replace";
	}

	static scalar_function_t GetFunction() {
		return regexp_replace_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return regexp_replace_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return regexp_replace_get_return_type;
	}

	static bind_scalar_function_t GetBindFunction() {
		return nullptr;
	}

	static dependency_function_t GetDependencyFunction() {
		return nullptr;
	}

	static bool HasSideEffects() {
		return false;
	}
};

} // namespace duckdb
