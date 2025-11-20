#pragma once
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

struct LimitPercentResult {
	bool is_percent = false;
	unique_ptr<ParsedExpression> expression;
};
} // namespace duckdb
