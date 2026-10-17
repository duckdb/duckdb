#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

class LeastGreatestSimplificationRule : public Rule {
public:
	explicit LeastGreatestSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb
