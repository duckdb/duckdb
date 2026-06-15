#pragma once

#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

struct IsDistinctFromTail {
	ExpressionType comparison_type;
	unique_ptr<ParsedExpression> expression;
};

struct ComparisonExpressionTail {
	ExpressionType comparison_type;
	vector<bool> not_keywords;
	unique_ptr<ParsedExpression> expression;
};

struct BetweenInLikeOperator {
	bool has_not = false;
	unique_ptr<ParsedExpression> expression;
};

} // namespace duckdb
