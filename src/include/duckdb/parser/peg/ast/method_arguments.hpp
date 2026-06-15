#pragma once

#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"

namespace duckdb {

struct MethodArguments {
	bool distinct = false;
	vector<FunctionArgument> arguments;
	vector<OrderByNode> order_bys;
	bool has_ignore_nulls = false;
	bool ignore_nulls = false;
};

} // namespace duckdb
