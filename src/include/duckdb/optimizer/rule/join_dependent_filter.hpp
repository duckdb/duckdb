//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/join_filter_derivation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

//! Join-dependent filters cannot be pushed down, but we can sometimes derive a filter from them that can
class JoinDependentFilterRule : public Rule {
public:
	explicit JoinDependentFilterRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb
