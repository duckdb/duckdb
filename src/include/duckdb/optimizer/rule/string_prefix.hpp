//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/left_to_prefix.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// This rule rewrites equality comparisons on extracted string prefixes into prefix comparisons,
// which can be pushed into the scan as range filters.
class StringPrefixRule : public Rule {
public:
	explicit StringPrefixRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb
