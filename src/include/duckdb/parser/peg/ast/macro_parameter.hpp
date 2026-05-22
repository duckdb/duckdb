#pragma once
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
struct MacroParameter {
	unique_ptr<ParsedExpression> expression;
	string name;
	LogicalType type = LogicalType::UNKNOWN;
	bool is_default = false;
	// PG-compat: bare `TYPE` (no name part) collides with DuckDB's named
	// parameter form. We mark `(TYPE)` as ambiguous in TransformSimpleParameter
	// and resolve later: with LANGUAGE present, treat it as a positional type
	// (auto-name $N); without LANGUAGE, keep as a parameter named TYPE.
	bool ambiguous_bare_type = false;
};

} // namespace duckdb
