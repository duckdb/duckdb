#pragma once
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
struct DistinctClause {
	bool is_distinct;
	vector<unique_ptr<ParsedExpression>> distinct_targets;
};
} // namespace duckdb
