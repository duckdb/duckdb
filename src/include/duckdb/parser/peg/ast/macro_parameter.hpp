#pragma once
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
struct MacroParameter {
	unique_ptr<ParsedExpression> expression;
	Identifier name;
	LogicalType type = LogicalType::UNKNOWN;
	bool is_default = false;
};

} // namespace duckdb
