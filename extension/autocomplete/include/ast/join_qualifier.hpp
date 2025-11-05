#pragma once
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
struct JoinQualifier {
	unique_ptr<ParsedExpression> on_clause;
	vector<string> using_columns;
};

} // namespace duckdb
