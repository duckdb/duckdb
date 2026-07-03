#pragma once
#include "duckdb/parser/parsed_expression.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {
struct JoinQualifier {
	unique_ptr<ParsedExpression> on_clause;
	vector<Identifier> using_columns;
};

} // namespace duckdb
