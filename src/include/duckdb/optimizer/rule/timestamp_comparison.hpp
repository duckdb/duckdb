//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/timestamp_comparison.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

namespace duckdb {

class TimeStampComparison : public Rule {
public:
	explicit TimeStampComparison(ClientContext &context, ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;

	unique_ptr<Expression> ApplyRule(BoundFunctionExpression *expr, ScalarFunction function, string pattern,
	                                 bool is_not_like);

private:
	ClientContext &context;
};

} // namespace duckdb
